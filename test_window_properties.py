"""
test_window_properties.py -- Randomised property and integration tests for Part 2
==================================================================================

TEAM / AUTHORSHIP
-----------------
Author ........: Ali Hadi Zbeeb   (EECE 350 -- Computer Networks, Group 9)
Part tested ...: Part 2 -- sender sending window, timers and retransmission
File written .: Ali Hadi Zbeeb
AI disclosure : Drafted with the help of an AI coding assistant (MiniMax M3)
                used by Ali Hadi Zbeeb, as required by the course assignment.

WHY THIS FILE EXISTS
--------------------
``test_window.py`` pins down every individual rule of CONTRACT.md section 6.
This file attacks the same code from the other side: it runs the window
against a scripted lossy/reordering channel and a reference receiver model
with randomised parameters, and checks the invariants of contract sections 6
and 9 after EVERY public call:

    0 <= base <= next_seq <= P
    next_seq - base <= N
    transmissions == sum of per packet attempts
    retransmissions == timeouts + fast_retransmits
    the timer runs exactly while packets are outstanding
    a fast retransmit happens at most once per base value
    the transfer always completes and the receiver gets every packet once,
    in order, even with loss and reordering

Run with:

    python3 -m unittest -v test_window_properties.py
"""

from __future__ import annotations

import os
import random
import sys
import types
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

if "protocol" not in sys.modules:
    try:  # pragma: no cover - depends on the repository state
        import protocol  # noqa: F401
    except ModuleNotFoundError:
        _stub = types.ModuleType("protocol")
        _stub.DATA = 1
        _stub.ACK = 2
        _stub.END = 3
        _stub.CHUNK_SIZE = 1024
        _stub.MAX_WINDOW = 4096
        sys.modules["protocol"] = _stub

from window import SenderWindow  # noqa: E402


PACKETS = 20


def make_packets(count: int):
    return [("PKT%04d" % index).encode("ascii") for index in range(count)]


class ScriptedChannel:
    """Part 1's channel behaviour (contract section 7), scripted and random.

    * a submission is dropped with probability ``loss``;
    * a surviving submission is held with probability ``reorder`` and released
      after the next surviving submission (the held packet arrives second);
    * a held packet that is never followed by another submission is released
      by :meth:`tick` once its hold deadline (``now + 0.1``) expires.

    The channel is Part 1's property; this model only exists so that Part 2
    can be exercised against realistic loss and reordering.
    """

    HOLD_SECONDS = 0.1

    def __init__(self, packets, rng, loss=0.1, reorder=0.1):
        self.packets = list(packets)
        self.rng = rng
        self.loss = loss
        self.reorder = reorder
        self.held = None
        self.hold_deadline = None
        self.delivered = []
        self.attempts = {}

    # -- the channel interface used by the window ------------------------
    def send(self, datagram: bytes, now: float) -> None:
        seq = int(datagram[3:7])
        self.attempts[seq] = self.attempts.get(seq, 0) + 1

        if self.rng.random() < self.loss:
            # Contract section 7: a dropped submission does NOT release an
            # unexpired hold.
            return

        if self.held is not None:
            # Contract section 7: the new packet goes out first, the held one
            # follows, so the pair arrives reversed.
            self.delivered.append(seq)
            self.delivered.append(self.held)
            self.held = None
            self.hold_deadline = None
            return

        if self.rng.random() < self.reorder:
            self.held = seq
            self.hold_deadline = now + self.HOLD_SECONDS
        else:
            self.delivered.append(seq)

    def tick(self, now: float) -> None:
        """Release an expired hold (contract section 7 rule 4)."""
        if self.held is not None and self.hold_deadline is not None and now >= self.hold_deadline:
            self.delivered.append(self.held)
            self.held = None
            self.hold_deadline = None

    def flush(self):
        """Release the hold immediately (used by the demo)."""
        if self.held is not None:
            self.delivered.append(self.held)
            self.held = None
            self.hold_deadline = None


class ReferenceReceiver:
    """Reference model of Part 3's cumulative-ACK receiver (contract section 4)."""

    def __init__(self, max_window=4096):
        self.expected = 0
        self.buffer = set()
        self.max_window = max_window
        self.acks = []
        self.written = []

    def receive(self, seq: int) -> int:
        if seq == self.expected:
            self.written.append(seq)
            self.expected += 1
            while self.expected in self.buffer:
                self.buffer.discard(self.expected)
                self.written.append(self.expected)
                self.expected += 1
        elif self.expected < seq < self.expected + self.max_window:
            self.buffer.add(seq)
        # Duplicates and packets outside the receive window are discarded.
        ack = self.expected
        self.acks.append(ack)
        return ack


def format_event(event) -> str:
    """Render one window event the way the terminal display should show it."""
    name = event["event"]
    if name == "SEND":
        return f"SEND {event['seq']}"
    if name == "ACK":
        return f"ACK {event['ack_next']}"
    if name == "DUP_ACK":
        return f"DUP ACK {event['ack_next']} (count {event['dup_ack_count']})"
    if name == "TIMEOUT":
        return f"TIMEOUT {event['seq']} (RTO {event['rto']:.2f}s)"
    if name == "RETRANSMIT":
        return f"RETRANSMIT {event['seq']} (attempt {event['attempt']}) - {event['reason']}"
    if name == "FAST_RETRANSMIT":
        return f"3 DUP ACKS -> RETRANSMIT {event['seq']}"
    if name == "RTO_BACKOFF":
        return f"RTO BACKOFF -> {event['rto']:.2f}s"
    if name == "WINDOW":
        last = event["base"] + event["window_size"] - 1
        return f"WINDOW [{event['base']} - {last}]"
    if name == "COMPLETE":
        return "TRANSPORT COMPLETE; hash result shown at receiver"
    if name == "STALE_ACK":
        return f"STALE ACK {event['ack_next']} ignored"
    if name == "ACK_UNSENT":
        return f"ACK {event['ack_next']} for unsent data ignored"
    return f"{name} {event.get('seq', '')}".strip()


def demo(seed=7, window_size=8, loss=0.1, reorder=0.1, packet_count=24):
    """Standalone Part 2 demo: window + scripted lossy channel + reference
    receiver.  Run with ``python3 test_window_properties.py --demo``."""
    rng = random.Random(seed)
    packets = make_packets(packet_count)
    channel = ScriptedChannel(packets, rng, loss=loss, reorder=reorder)
    receiver = ReferenceReceiver()
    events = []

    window = SenderWindow(
        packets,
        window_size=window_size,
        send_packet=channel.send,
        emit=events.append,
    )

    print("=" * 72)
    print("PART 2 DEMO -- sending window, timers and retransmission (Ali Hadi Zbeeb)")
    print("=" * 72)
    print(f"packets (incl. END): {packet_count}   window N: {window_size}   "
          f"loss: {loss}   reorder: {reorder}   seed: {seed}")
    print("-" * 72)

    now = 1000.0
    seen = 0
    window.fill_window(now)
    for event in events:
        print(f"[SENDER] {format_event(event)}")
    events.clear()

    iterations = 0
    while not window.done() and iterations < 100000:
        iterations += 1
        now += 0.01
        while channel.delivered:
            seq = channel.delivered.pop(0)
            ack = receiver.receive(seq)
            window.on_ack(ack, now)
        if channel.held is not None and now % 0.1 < 0.01:
            channel.flush()
        window.on_tick(now)
        window.fill_window(now)
        for event in events:
            print(f"[SENDER] {format_event(event)}")
        events.clear()

    stats = window.stats()
    print("-" * 72)
    print("SENDER SUMMARY (contract section 9)")
    for key in ("transmissions", "retransmissions", "timeouts", "fast_retransmits",
                "acked_packets", "transfer_time_s"):
        print(f"  {key:<18}: {stats[key]}")
    print(f"  {'srtt / rttvar':<18}: {stats['srtt']} / {stats['rttvar']}")
    print(f"  {'rto':<18}: {stats['rto']}")
    print("-" * 72)
    print(f"RECEIVER: received {len(receiver.written)} packets, expected next = "
          f"{receiver.expected}, last ACK = {receiver.acks[-1]}")
    print("=" * 72)
    return stats


class TestWindowProperties(unittest.TestCase):
    def _run(self, seed, window_size, loss, reorder, packet_count=PACKETS, step=0.01):
        rng = random.Random(seed)
        packets = make_packets(packet_count)
        channel = ScriptedChannel(packets, rng, loss=loss, reorder=reorder)
        receiver = ReferenceReceiver()
        events = []
        window = SenderWindow(
            packets,
            window_size=window_size,
            send_packet=channel.send,
            emit=events.append,
        )

        now = 1000.0
        window.fill_window(now)

        fast_retransmits_per_base = {}
        event_cursor = 0
        iterations = 0

        def check_invariants():
            """Assert the contract invariants of sections 6 and 9."""
            snapshot = window.snapshot()
            stats = window.stats()
            base = snapshot["base"]
            next_seq = snapshot["next_seq"]

            self.assertGreaterEqual(base, 0)
            self.assertLessEqual(base, next_seq)
            self.assertLessEqual(next_seq, packet_count)
            self.assertLessEqual(next_seq - base, window_size)
            self.assertEqual(stats["retransmissions"],
                             stats["timeouts"] + stats["fast_retransmits"])
            self.assertEqual(sum(channel.attempts.values()), stats["transmissions"])
            self.assertEqual(stats["acked_packets"], base)
            self.assertEqual(window.done(), base == packet_count)

            # The single timer exists exactly while packets are outstanding.
            if base < next_seq:
                self.assertIsNotNone(snapshot["timer_deadline"])
            else:
                self.assertIsNone(snapshot["timer_deadline"])

        while not window.done() and iterations < 200000:
            iterations += 1
            now += step

            while channel.delivered:
                seq = channel.delivered.pop(0)
                window.on_ack(receiver.receive(seq), now)
                # The most dangerous moment: the ACK may have just emptied the
                # window, in which case the timer must be stopped.
                check_invariants()

            # Release a held packet once its 0.1 s hold deadline expires.
            channel.tick(now)

            window.on_tick(now)
            window.fill_window(now)

            # ---- invariants after every public call --------------------
            check_invariants()

            # At most one fast retransmit per base value (only NEW events).
            while event_cursor < len(events):
                event = events[event_cursor]
                event_cursor += 1
                if event["event"] == "FAST_RETRANSMIT":
                    seen = fast_retransmits_per_base.setdefault(event["seq"], 0)
                    fast_retransmits_per_base[event["seq"]] = seen + 1
                    self.assertEqual(seen, 0)

        self.assertTrue(window.done(), "transfer did not complete")
        self.assertLess(iterations, 200000, "sender loop did not terminate")

        # The receiver must have every packet exactly once, in order.
        self.assertEqual(receiver.written, list(range(packet_count)))
        self.assertEqual(receiver.expected, packet_count)
        self.assertEqual(receiver.acks[-1], packet_count)

        stats = window.stats()
        self.assertEqual(stats["transmissions"],
                         packet_count + stats["retransmissions"])
        return stats

    def test_no_loss_no_reorder(self):
        for window_size in (1, 2, 3, 8, 16):
            with self.subTest(window_size=window_size):
                stats = self._run(1, window_size, 0.0, 0.0)
                self.assertEqual(stats["retransmissions"], 0)
                self.assertEqual(stats["timeouts"], 0)
                self.assertEqual(stats["fast_retransmits"], 0)

    def test_random_loss_and_reordering(self):
        for seed in range(25):
            with self.subTest(seed=seed):
                rng = random.Random(seed)
                window_size = rng.randint(1, 16)
                loss = rng.uniform(0.0, 0.4)
                reorder = rng.uniform(0.0, 0.4)
                self._run(seed + 1000, window_size, loss, reorder)

    def test_heavy_loss(self):
        for seed in range(10):
            with self.subTest(seed=seed):
                # A coarser simulated clock step keeps the RTO backoff (which
                # can reach 60 s) from making this case slow.
                self._run(seed + 5000, 4, 0.5, 0.2, step=0.05)

    def test_window_size_one_survives_reordering(self):
        for seed in range(10):
            with self.subTest(seed=seed):
                self._run(seed + 9000, 1, 0.1, 0.5)


if __name__ == "__main__":
    if "--demo" in sys.argv:
        demo()
    else:
        unittest.main(verbosity=2)
