"""
window.py -- Reliable UDP transfer, PART 2 of 3 (sender window / timers / retransmission)
=========================================================================================

TEAM / AUTHORSHIP
-----------------
Part owner ....: Ali Hadi Zbeeb          (EECE 350 -- Computer Networks, Group 9)
File written .: Ali Hadi Zbeeb
Part covered .: Part 2 -- "sender, window and retransmission"

This file contains ONLY Part 2 code.  It does not touch sockets, files, or the
packet format:

  * Part 1 (Yara El Rohayem / Ghadi Zouki) owns ``protocol.py``,
    ``packetizer.py``, ``channel.py`` and the sender CLI loop.
  * Part 3 (third teammate) owns ``receiver.py``.

INTERFACE OWNERSHIP
-------------------
Part 1 hands this module a tuple of already-encoded datagrams and a bound
``channel.send`` callable.  Part 1 feeds validated cumulative ACK numbers back
in through :meth:`SenderWindow.on_ack`.  This module never calls
``socket.sendto``; every original transmission and every retransmission is
handed to the channel, which is the single owner of the loss/reorder simulator.

AI-ASSISTANCE DISCLOSURE (required by the course assignment)
-------------------------------------------------------------
This file was drafted and reviewed with the help of an AI coding assistant
(MiniMax M3) used by Ali Hadi Zbeeb.  All design decisions follow the team's
integration contract ("CONTRACT.md", version 1.0), all rules were implemented
deliberately, and the accompanying ``test_window.py`` suite was written to
prove each contract rule.  The AI assistant did not run the live demo and is
not responsible for the group's final submission.

CONTRACT RULES IMPLEMENTED HERE (see CONTRACT.md sections 6, 8 and 9)
--------------------------------------------------------------------
State kept by the window:

    base      oldest unacknowledged sequence number
    next_seq  next never-submitted sequence number
    N         maximum number of outstanding unique packets (window size)
    dup_ack_count
              number of ACKs equal to the current base while data is outstanding
    fast_retransmitted_base
              base value for which a fast retransmit already fired (guard)
    timer_deadline
              absolute monotonic deadline of the single oldest-packet timer

Invariants that always hold:

    0 <= base <= next_seq <= P            (P = number of packets, END included)
    next_seq - base <= N

Cumulative ACK rule (contract section 4): ACK ``k`` means "every packet with
sequence < k arrived contiguously and k is the next one I need".  Therefore a
valid ACK ``k`` with ``base < k <= next_seq`` acknowledges the whole run
``[base, k)`` and moves ``base`` to ``k``.

Timeout / fast retransmit rule (contract section 6): this is a *selective*
repeat scheme, NOT Go-Back-N.  On timeout or fast retransmit only the packet
with sequence number ``base`` is resent.  The END packet is an ordinary window
member and is retransmitted exactly like DATA.

RTO rules (contract section 6, inspired by RFC 6298 sections 2, 3 and 5):
initial RTO 1 s, clamped to [1, 60] s, clock granularity G = 0.01 s,
SRTT/RTTVAR updated as specified, and an RTT sample is taken ONLY when an ACK
advances the window by exactly one packet whose ``attempts == 1``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

try:  # Part 1 owns protocol.py; in the group repository this import always wins.
    from protocol import MAX_WINDOW
except ModuleNotFoundError:  # pragma: no cover - isolated Part 2 unit-test runs
    # Fallback ONLY so that Part 2 can be unit-tested before Part 1's
    # protocol.py exists.  In the shared repository protocol.py is always
    # present and this constant is imported from there (contract section 2).
    MAX_WINDOW = 4096


__all__ = [
    "SenderWindow",
    "ACKNOWLEDGED",
    "RETRANSMITTED",
    "SENT",
    "UNSENT",
    "INITIAL_RTO",
    "MIN_RTO",
    "MAX_RTO",
    "CLOCK_GRANULARITY",
    "DUP_ACK_THRESHOLD",
]


# ---------------------------------------------------------------------------
# Constants (contract section 6: "Ali's sending-window contract")
# ---------------------------------------------------------------------------
INITIAL_RTO = 1.0          # seconds; contract: "Initial RTO = 1 second"
MIN_RTO = 1.0              # seconds; lower clamp
MAX_RTO = 60.0             # seconds; upper clamp
CLOCK_GRANULARITY = 0.01   # seconds; G in RFC 6298 terms
RTT_ALPHA = 0.125          # SRTT = (1 - alpha) * SRTT + alpha * R
RTT_BETA = 0.25            # RTTVAR = beta * |SRTT - R| + (1 - beta) * RTTVAR
RTTVAR_BETA = 0.75
DUP_ACK_THRESHOLD = 3      # three equal ACKs -> one fast retransmit

# Display states of a window slot (contract section 6).
ACKNOWLEDGED = "ACKNOWLEDGED"
RETRANSMITTED = "RETRANSMITTED"
SENT = "SENT"
UNSENT = "UNSENT"


@dataclass
class _PacketRecord:
    """Per-packet bookkeeping kept by the window for one submitted packet."""

    seq: int
    attempts: int = 0
    first_sent_at: Optional[float] = None
    last_sent_at: Optional[float] = None
    acked: bool = False

    @property
    def state(self) -> str:
        # Precedence required by the contract: ACKNOWLEDGED wins over
        # RETRANSMITTED, RETRANSMITTED wins over SENT.  Retransmission
        # history is preserved after the packet has been acknowledged.
        if self.acked:
            return ACKNOWLEDGED
        if self.attempts > 1:
            return RETRANSMITTED
        return SENT


class SenderWindow:
    """Cumulative-ACK sending window with RFC 6298 style timeout and fast retransmit.

    The window never touches the network itself: every transmission (original
    or retransmission) is handed to the ``send_packet`` callback, which Part 1
    binds to ``UnreliableChannel.send``.  Delivery is learned ONLY from ACKs.

    Public interface (contract section 8)::

        SenderWindow(packets, *, window_size, send_packet, emit)
            .fill_window(now) -> None
            .on_ack(ack_next, now) -> None
            .on_tick(now) -> None
            .done() -> bool
            .snapshot() -> dict
            .stats() -> dict
    """

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------
    def __init__(
        self,
        packets: Sequence[bytes],
        *,
        window_size: int,
        send_packet: Callable[[bytes, float], None],
        emit: Optional[Callable[[dict], None]],
    ) -> None:
        """Create the window.  The constructor performs no transmission.

        Args:
            packets: the prepared, already-encoded datagrams in sequence order.
                Index == sequence number; the last item is the END packet.
                ``P = len(packets)`` includes the END packet.
            window_size: maximum number of outstanding unique packets ``N``.
                Must satisfy ``1 <= N <= MAX_WINDOW`` (contract section 2).
            send_packet: bound ``channel.send`` method with signature
                ``(bytes, float) -> None``.  It must not call :meth:`on_ack`
                synchronously (contract section 8).
            emit: shared logging callback ``(dict) -> None``; ``None`` disables
                logging.

        Raises:
            ValueError: on an invalid configuration.  The check happens before
                the transfer starts, so a rejected window never sends anything.
        """
        packet_list = list(packets)
        if not packet_list:
            raise ValueError(
                "packets must contain at least the END packet (contract: END always exists)"
            )
        for index, datagram in enumerate(packet_list):
            if not isinstance(datagram, (bytes, bytearray, memoryview)):
                raise ValueError(
                    f"packet {index} must be an immutable bytes datagram, "
                    f"got {type(datagram).__name__}"
                )
        if isinstance(window_size, bool) or not isinstance(window_size, int):
            raise ValueError("window_size must be an integer")
        if not 1 <= window_size <= MAX_WINDOW:
            raise ValueError(
                f"window_size must satisfy 1 <= N <= {MAX_WINDOW}, got {window_size}"
            )
        if not callable(send_packet):
            raise ValueError("send_packet must be callable: (bytes, now) -> None")
        if emit is not None and not callable(emit):
            raise ValueError("emit must be callable: (event dict) -> None")

        # Immutable copies: the window must not be able to mutate its packets.
        self._packets: Tuple[bytes, ...] = tuple(bytes(p) for p in packet_list)
        self._packet_count = len(self._packets)
        self._window_size = window_size
        self._send_packet = send_packet
        self._emit = emit if emit is not None else (lambda event: None)

        # --- window state (contract section 6) ---
        self._base = 0
        self._next_seq = 0
        self._dup_ack_count = 0
        self._fast_retransmitted_base: Optional[int] = None
        self._timer_deadline: Optional[float] = None

        # --- timeout estimator ---
        self._rto = INITIAL_RTO
        self._srtt: Optional[float] = None
        self._rttvar: Optional[float] = None

        # --- per packet bookkeeping ---
        self._records: Dict[int, _PacketRecord] = {}

        # --- counters (contract section 9) ---
        self._transmissions = 0
        self._retransmissions = 0
        self._timeouts = 0
        self._fast_retransmits = 0

        # --- timing ---
        self._first_send_at: Optional[float] = None
        self._completed_at: Optional[float] = None
        self._last_now: Optional[float] = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def fill_window(self, now: float) -> None:
        """Submit never-sent packets until the window is full.

        New packets are allowed while ``next_seq < min(base + N, P)``.  Calling
        this twice can never resend a packet: only never-submitted sequence
        numbers are ever handed to the channel here.
        """
        self._touch(now)
        limit = min(self._base + self._window_size, self._packet_count)
        newly_sent: List[int] = []
        while self._next_seq < limit:
            seq = self._next_seq
            # Record the attempt and advance next_seq BEFORE handing the bytes
            # to the channel: a simulated drop or hold still counts as an
            # attempt (contract section 6).
            self._next_seq += 1
            self._submit_new(seq, now)
            newly_sent.append(seq)
        if newly_sent:
            self._emit_window_event(now, "WINDOW")

    def on_ack(self, ack_next: int, now: float) -> None:
        """Process one validated cumulative ACK from the configured peer.

        ``ack_next`` is the next sequence number the receiver still needs, i.e.
        every packet with a smaller sequence number arrived contiguously.

        * ``base < ack_next <= next_seq``: acknowledge ``[base, ack_next)``,
          move ``base`` forward, reset the duplicate counter and the fast
          retransmit guard, and restart the timer if anything is still
          outstanding.
        * ``ack_next == base`` with outstanding data: duplicate ACK.  The third
          one triggers exactly one fast retransmission of packet ``base``.
        * ``ack_next == base`` without outstanding data: ignored, it is not a
          duplicate.
        * ``ack_next < base``: stale ACK, ignored.
        * ``ack_next > next_seq``: ACK for data that was never sent, ignored.

        This method never fills the window; the sender loop calls
        :meth:`fill_window` for that (contract section 8).
        """
        self._touch(now)

        if isinstance(ack_next, bool) or not isinstance(ack_next, int) or ack_next < 0:
            self._emit_event(
                now, "ACK_IGNORED", reason="invalid acknowledgement value", ack_next=ack_next
            )
            return

        if ack_next < self._base:
            # Stale ACK: an old duplicate, must not touch any state.
            self._emit_event(
                now,
                "STALE_ACK",
                reason="acknowledgement older than base",
                ack_next=ack_next,
                base=self._base,
                next_seq=self._next_seq,
            )
            return

        if ack_next > self._next_seq:
            # ACK for a packet that was never submitted: impossible in a
            # healthy run, so it is logged and dropped.
            self._emit_event(
                now,
                "ACK_UNSENT",
                reason="acknowledgement beyond next_seq",
                ack_next=ack_next,
                base=self._base,
                next_seq=self._next_seq,
            )
            return

        if ack_next == self._base:
            if self._base >= self._next_seq:
                # Nothing outstanding: equal ACKs are not duplicates.
                self._emit_event(
                    now,
                    "ACK_IDLE",
                    reason="no outstanding packets",
                    ack_next=ack_next,
                    base=self._base,
                    next_seq=self._next_seq,
                )
                return

            self._dup_ack_count += 1
            self._emit_event(
                now,
                "DUP_ACK",
                reason="duplicate acknowledgement",
                seq=self._base,
                ack_next=ack_next,
                base=self._base,
                next_seq=self._next_seq,
                dup_ack_count=self._dup_ack_count,
                attempt=self._records[self._base].attempts,
                packet=self._packet_view(self._base),
            )
            if (
                self._dup_ack_count >= DUP_ACK_THRESHOLD
                and self._fast_retransmitted_base != self._base
            ):
                # Third duplicate ACK: one fast retransmission of the oldest
                # unacknowledged packet, then the guard blocks further fast
                # retransmits until base advances.
                self._fast_retransmitted_base = self._base
                self._fast_retransmits += 1
                self._retransmit(self._base, now, reason="fast retransmit after 3 duplicate ACKs")
                # Restart the timer with the current RTO; do NOT double it.
                self._timer_deadline = now + self._rto
                self._emit_event(
                    now,
                    "FAST_RETRANSMIT",
                    reason="3 duplicate ACKs",
                    seq=self._base,
                    base=self._base,
                    next_seq=self._next_seq,
                    rto=self._rto,
                    attempt=self._records[self._base].attempts,
                    packet=self._packet_view(self._base),
                )
            return

        # ---- advancing ACK: base < ack_next <= next_seq -------------------
        old_base = self._base
        self._update_rtt(old_base, ack_next, now)

        for seq in range(old_base, ack_next):
            record = self._records.get(seq)
            if record is not None:
                record.acked = True

        self._base = ack_next
        # An advancing ACK is never a duplicate: reset counter and guard.
        self._dup_ack_count = 0
        self._fast_retransmitted_base = None

        # Restart the timer only if something is still outstanding.
        if self._base < self._next_seq:
            self._timer_deadline = now + self._rto
        else:
            self._timer_deadline = None

        self._emit_event(
            now,
            "ACK",
            reason="cumulative acknowledgement",
            seq=ack_next - 1,
            ack_next=ack_next,
            base=self._base,
            next_seq=self._next_seq,
            acked=list(range(old_base, ack_next)),
        )
        self._emit_window_event(now, "WINDOW")

        if self._base == self._packet_count:
            # Completion is base == P, i.e. the END packet is acknowledged too.
            self._completed_at = now
            self._emit_event(
                now,
                "COMPLETE",
                reason="all packets acknowledged",
                base=self._base,
                next_seq=self._next_seq,
                total_packets=self._packet_count,
                transfer_time_s=self._elapsed(now),
            )

    def on_tick(self, now: float) -> None:
        """Handle an expired retransmission timer, at most once per call.

        A timeout retransmits ONLY the oldest unacknowledged packet (``base``),
        doubles the RTO (capped at 60 s) and restarts the deadline.
        """
        self._touch(now)
        if self._timer_deadline is None:
            return
        if now < self._timer_deadline:
            return
        if self._base >= self._next_seq:
            # Nothing outstanding any more (should not happen): stop the timer.
            self._timer_deadline = None
            return

        seq = self._base
        expired_rto = self._rto
        self._timeouts += 1
        self._emit_event(
            now,
            "TIMEOUT",
            reason="retransmission timer expired",
            seq=seq,
            base=self._base,
            next_seq=self._next_seq,
            rto=expired_rto,
            attempt=self._records[seq].attempts,
            # The packet state is the state at the moment the timer expired;
            # the following RETRANSMIT event carries the post-retry state.
            packet=self._packet_view(seq),
        )

        self._retransmit(seq, now, reason="timeout")

        # Back off: double the RTO (cap 60 s).  The backoff is only cleared by
        # a new eligible RTT sample (contract section 6).
        self._rto = min(self._rto * 2.0, MAX_RTO)
        self._timer_deadline = now + self._rto
        self._emit_event(
            now,
            "RTO_BACKOFF",
            reason="timeout backoff",
            seq=seq,
            base=self._base,
            next_seq=self._next_seq,
            rto=self._rto,
        )

    def done(self) -> bool:
        """True when every packet, END included, has been acknowledged."""
        return self._base == self._packet_count

    def snapshot(self) -> dict:
        """Return a copy of the current window state for the renderer.

        Keys: ``base``, ``next_seq``, ``window_size``, ``rto``,
        ``timer_deadline``, ``dup_ack_count`` and ``packets``.

        ``packets`` holds one record per current window slot (sequence numbers
        ``base .. base + N - 1``), each with ``seq``, ``state`` and
        ``attempts``.  ``acked_history`` is an extra display aid (not required
        by the contract) that lists the most recently acknowledged packets so
        the terminal view can show the ACKNOWLEDGED state as the window slides.
        """
        end = min(self._base + self._window_size, self._packet_count)
        packets = [self._slot_view(seq) for seq in range(self._base, end)]

        history_start = max(0, self._base - self._window_size)
        acked_history = [
            self._slot_view(seq) for seq in range(history_start, self._base)
        ]

        return {
            "base": self._base,
            "next_seq": self._next_seq,
            "window_size": self._window_size,
            "rto": self._rto,
            "timer_deadline": self._timer_deadline,
            "dup_ack_count": self._dup_ack_count,
            "packets": packets,
            "acked_history": acked_history,
        }

    def stats(self, now: Optional[float] = None) -> dict:
        """Return a copy of the sender-side counters (contract section 9).

        ``transmissions`` counts every submission to the channel (including
        dropped or held attempts), ``retransmissions`` counts submissions for
        an already attempted sequence number, ``acked_packets`` is ``base``,
        and ``transfer_time_s`` runs from the first submission to the final
        ACK.  The optional ``now`` argument is an extension: without it the
        last timestamp observed by the window is used.
        """
        reference = self._last_now if now is None else now
        return {
            "transmissions": self._transmissions,
            "retransmissions": self._retransmissions,
            "timeouts": self._timeouts,
            "fast_retransmits": self._fast_retransmits,
            "acked_packets": self._base,
            "transfer_time_s": self._elapsed(reference),
            # Extra, non-contractual but useful for the live display.
            "total_packets": self._packet_count,
            "outstanding": self._next_seq - self._base,
            "window_size": self._window_size,
            "dup_ack_count": self._dup_ack_count,
            "srtt": self._srtt,
            "rttvar": self._rttvar,
            "rto": self._rto,
            "done": self.done(),
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _touch(self, now: float) -> None:
        """Remember the newest timestamp seen (used for "elapsed so far")."""
        self._last_now = now

    def _elapsed(self, now: Optional[float]) -> float:
        """Transfer time: first submission -> final ACK (0.0 before the start)."""
        if self._first_send_at is None:
            return 0.0
        end = self._completed_at if self._completed_at is not None else now
        if end is None:
            return 0.0
        return max(0.0, end - self._first_send_at)

    def _submit_new(self, seq: int, now: float) -> None:
        """Record and submit one never-sent packet."""
        record = _PacketRecord(seq=seq, attempts=1, first_sent_at=now, last_sent_at=now)
        self._records[seq] = record

        self._transmissions += 1
        if self._first_send_at is None:
            self._first_send_at = now

        # The first outstanding send starts the single timer for the oldest
        # outstanding packet.  Later sends do NOT restart it.
        if self._timer_deadline is None:
            self._timer_deadline = now + self._rto

        self._emit_event(
            now,
            "SEND",
            reason="new packet",
            seq=seq,
            attempt=record.attempts,
            base=self._base,
            next_seq=self._next_seq,
            packet=self._packet_view(seq),
        )
        # Every transmission goes through the channel; Part 2 never uses a
        # socket directly.
        self._send_packet(self._packets[seq], now)

    def _retransmit(self, seq: int, now: float, *, reason: str) -> None:
        """Resend exactly one packet (the oldest unacknowledged one)."""
        record = self._records[seq]
        record.attempts += 1
        record.last_sent_at = now
        self._transmissions += 1
        self._retransmissions += 1
        self._emit_event(
            now,
            "RETRANSMIT",
            reason=reason,
            seq=seq,
            attempt=record.attempts,
            base=self._base,
            next_seq=self._next_seq,
            dup_ack_count=self._dup_ack_count,
            rto=self._rto,
            packet=self._packet_view(seq),
        )
        # Retransmissions take the same path as original transmissions and are
        # therefore subject to the same loss/reorder simulation.
        self._send_packet(self._packets[seq], now)

    def _update_rtt(self, old_base: int, ack_next: int, now: float) -> None:
        """Update SRTT / RTTVAR / RTO from an eligible RTT sample.

        A sample is taken only when the ACK advances the window by exactly one
        packet and that packet was never retransmitted (``attempts == 1``).
        Cumulative jumps and retransmitted packets are skipped because their
        measured time would be ambiguous.
        """
        if ack_next != old_base + 1:
            return
        record = self._records.get(old_base)
        if record is None or record.attempts != 1 or record.first_sent_at is None:
            return
        sample = now - record.first_sent_at
        if sample < 0:
            return

        if self._srtt is None or self._rttvar is None:
            self._srtt = sample
            self._rttvar = sample / 2.0
        else:
            # RTTVAR uses the OLD SRTT; SRTT is updated afterwards.
            self._rttvar = RTTVAR_BETA * self._rttvar + RTT_BETA * abs(self._srtt - sample)
            self._srtt = (1.0 - RTT_ALPHA) * self._srtt + RTT_ALPHA * sample

        self._rto = self._clamp_rto(
            self._srtt + max(CLOCK_GRANULARITY, 4.0 * self._rttvar)
        )

    @staticmethod
    def _clamp_rto(value: float) -> float:
        """Clamp an RTO candidate to the permitted [1, 60] second range."""
        return max(MIN_RTO, min(MAX_RTO, value))

    def _packet_view(self, seq: int) -> Dict[str, Any]:
        """Copy of one packet's public record (safe to hand to the renderer)."""
        record = self._records.get(seq)
        if record is None:
            return {"seq": seq, "state": UNSENT, "attempts": 0}
        return {
            "seq": record.seq,
            "state": record.state,
            "attempts": record.attempts,
            "acked": record.acked,
        }

    def _slot_view(self, seq: int) -> Dict[str, Any]:
        """Window-slot record: ``seq``, ``state`` and ``attempts``."""
        record = self._records.get(seq)
        if record is None:
            return {"seq": seq, "state": UNSENT, "attempts": 0}
        return {"seq": record.seq, "state": record.state, "attempts": record.attempts}

    def _emit_window_event(self, now: float, event: str) -> None:
        """Emit a window event including the packet states."""
        self._emit_event(
            now,
            event,
            reason="window state",
            base=self._base,
            next_seq=self._next_seq,
            window_size=self._window_size,
            total_packets=self._packet_count,
            packets=[self._slot_view(seq) for seq in range(self._base, min(
                self._base + self._window_size, self._packet_count
            ))],
        )

    def _emit_event(self, now: float, event: str, **fields: Any) -> None:
        """Send one event to the shared logging callback.

        Every event carries ``time`` (monotonic float), ``side`` ("sender") and
        ``event`` (string), plus the applicable protocol fields.
        """
        payload: Dict[str, Any] = {"time": float(now), "side": "sender", "event": event}
        payload.update(fields)
        self._emit(payload)
