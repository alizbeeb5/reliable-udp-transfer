# Component owner: Ali Hadi Zbeeb — EECE 350, Person 2.
# Initial implementation generated with OpenAI Codex assistance, 2026-11-10.
# Ownership is the task allocation, not a claim of unaided student authorship.
"""Socket-free sending window implementing CONTRACT.md sections 6, 8 and 9.

Call from one event loop, with time.monotonic() timestamps in seconds.
Packets are opaque immutable bytes, ordered by sequence, with END last.
The caller validates ACK datagrams and peers before calling on_ack().
Callbacks must not re-enter this controller; callback errors are fatal.
"""

from dataclasses import dataclass
import time


# Mirrors CONTRACT.md while Person 1's shared protocol.py is not yet present.
# At integration this can become: from protocol import MAX_WINDOW
MAX_WINDOW = 4096
MIN_RTO = 1.0
MAX_RTO = 60.0
CLOCK_GRANULARITY = 0.01


@dataclass
class _PacketState:
    attempts: int = 0
    first_sent_at: float = None
    last_sent_at: float = None
    acked: bool = False

    @property
    def state(self):
        if self.acked:
            return "ACKNOWLEDGED"
        if self.attempts > 1:
            return "RETRANSMITTED"
        return "SENT" if self.attempts else "UNSENT"


class SenderWindow:
    """A cumulative-ACK window; retry only the oldest missing packet.

    send_packet(datagram, now) must be the unreliable channel callback.
    emit(event) receives independent dictionaries for a display/event log.
    Neither callback should mutate this controller or synchronously deliver ACKs.
    """

    def __init__(self, packets, *, window_size, send_packet, emit):
        if type(window_size) is not int or not 1 <= window_size <= MAX_WINDOW:
            raise ValueError("window_size must be an integer from 1 to 4096")
        if not callable(send_packet) or not callable(emit):
            raise ValueError("send_packet and emit must be callable")
        if isinstance(packets, (bytes, bytearray)):
            raise ValueError("packets must be a sequence of encoded datagrams")
        try:
            packets = tuple(packets)
        except TypeError as exc:
            raise ValueError("packets must be iterable") from exc
        if not packets or any(not isinstance(p, bytes) or not p for p in packets):
            raise ValueError("provide nonempty bytes datagrams, including final END")
        if len(packets) > 2**32 - 1:
            raise ValueError("final ACK must fit the 32-bit sequence field")

        self._packets = packets
        self._records = [_PacketState() for _ in packets]
        self._send_packet = send_packet
        self._emit = emit
        self.window_size = window_size
        self.base = 0
        self.next_seq = 0
        self.dup_ack_count = 0
        self.fast_retransmitted_base = None
        self.timer_deadline = None
        self.srtt = None
        self.rttvar = None
        self.rto = MIN_RTO
        self._started_at = None
        self._finished_at = None
        self._transmissions = 0
        self._retransmissions = 0
        self._timeouts = 0
        self._fast_retransmits = 0

    def fill_window(self, now):
        """Submit each new packet once while the current window has space."""
        changed = False
        limit = min(self.base + self.window_size, len(self._packets))
        while self.next_seq < limit:
            seq = self.next_seq
            # Advance BEFORE submission: drop/hold still consumes this slot.
            self.next_seq += 1
            self._transmit(seq, now)
            changed = True
        if changed:
            self._log_window(now)

    def on_ack(self, ack_next, now):
        """Apply a validated cumulative ACK; leave refilling to the caller."""
        if type(ack_next) is not int or ack_next < 0 or ack_next > self.next_seq:
            self._log(now, "IGNORED_ACK", ack_next=ack_next, reason="invalid_or_unsent")
            return
        if ack_next < self.base:
            self._log(now, "IGNORED_ACK", ack_next=ack_next, reason="stale")
            return
        if ack_next == self.base:
            if self.base == self.next_seq:
                return  # Equal ACK with nothing outstanding is not a duplicate.
            self.dup_ack_count += 1
            self._log(now, "DUP_ACK", ack_next=ack_next, count=self.dup_ack_count)
            if self.dup_ack_count == 3 and self.fast_retransmitted_base != self.base:
                self.fast_retransmitted_base = self.base
                self._fast_retransmits += 1
                self.timer_deadline = now + self.rto
                self._transmit(self.base, now, reason="fast_retransmit")
                self._log_window(now)
            return

        old_base = self.base
        record = self._records[old_base]
        # Conservative Karn rule: no samples from retries or cumulative jumps.
        if ack_next == old_base + 1 and record.attempts == 1:
            self._update_rtt(now - record.first_sent_at, now)
        for seq in range(old_base, ack_next):
            self._records[seq].acked = True
        self.base = ack_next
        self.dup_ack_count = 0
        self.fast_retransmitted_base = None
        self.timer_deadline = now + self.rto if self.base < self.next_seq else None
        if self.done():
            self._finished_at = now
        self._log(now, "ACK", ack_next=ack_next, acknowledged_from=old_base,
                  acknowledged_through=ack_next - 1, state="ACKNOWLEDGED")
        self._log_window(now)
        if self.done():
            self._log(now, "COMPLETE", ack_next=ack_next,
                      message="TRANSPORT COMPLETE; hash result shown at receiver")

    def on_tick(self, now):
        """Handle at most one expiry; call after processing available ACKs."""
        if self.timer_deadline is None or now < self.timer_deadline:
            return
        self._timeouts += 1
        self._log(now, "TIMEOUT", seq=self.base, reason="oldest_unacknowledged")
        self.rto = min(MAX_RTO, 2 * self.rto)
        self.timer_deadline = now + self.rto
        self._transmit(self.base, now, reason="timeout")
        self._log_window(now)

    def done(self):
        """True only after ACK of the final packet (END)."""
        return self.base == len(self._packets)

    def snapshot(self):
        """Return independent display records for the current window slots."""
        return {
            "base": self.base, "next_seq": self.next_seq,
            "window_size": self.window_size, "rto": self.rto,
            "timer_deadline": self.timer_deadline,
            "dup_ack_count": self.dup_ack_count,
            "packets": [
                {"seq": seq, "state": self._records[seq].state,
                 "attempts": self._records[seq].attempts}
                for seq in range(self.base, min(self.base + self.window_size,
                                                len(self._packets)))
            ],
        }

    def stats(self):
        """Return counters and time from first submission to final ACK.

        During a live transfer the elapsed time is sampled with monotonic().
        Tests using artificial times should inspect duration after completion
        or patch this module's clock when inspecting an active transfer.
        """
        elapsed = 0.0
        if self._started_at is not None:
            end = self._finished_at if self._finished_at is not None else time.monotonic()
            elapsed = max(0.0, end - self._started_at)
        return {
            "total_packets": len(self._packets),
            "transmissions": self._transmissions,
            "retransmissions": self._retransmissions,
            "timeouts": self._timeouts,
            "fast_retransmits": self._fast_retransmits,
            "acked_packets": self.base,
            "transfer_time_s": elapsed,
            "complete": self.done(),
        }

    def _transmit(self, seq, now, reason=None):
        record = self._records[seq]
        retry = record.attempts > 0
        record.attempts += 1
        record.last_sent_at = now
        if not retry:
            record.first_sent_at = now
        if self._started_at is None:
            self._started_at = now
        self._transmissions += 1
        if retry:
            self._retransmissions += 1
        if self.timer_deadline is None:
            self.timer_deadline = now + self.rto
        self._log(now, "RETRANSMIT" if retry else "SEND", seq=seq,
                  attempt=record.attempts, reason=reason)
        # No sendto, and no inspecting simulator delivery decisions here.
        self._send_packet(self._packets[seq], now)

    def _update_rtt(self, sample, now):
        if self.srtt is None:
            self.srtt = sample
            self.rttvar = sample / 2
        else:
            # This deviation MUST use the old SRTT, before updating it.
            self.rttvar = 0.75 * self.rttvar + 0.25 * abs(self.srtt - sample)
            self.srtt = 0.875 * self.srtt + 0.125 * sample
        estimate = self.srtt + max(CLOCK_GRANULARITY, 4 * self.rttvar)
        self.rto = min(MAX_RTO, max(MIN_RTO, estimate))
        self._log(now, "RTT", sample=sample, srtt=self.srtt, rttvar=self.rttvar)

    def _log(self, now, event, **fields):
        self._emit({"time": now, "side": "sender", "event": event,
                    "base": self.base, "next_seq": self.next_seq,
                    "rto": self.rto, **fields})

    def _log_window(self, now):
        self._log(now, "WINDOW", **self.snapshot())
