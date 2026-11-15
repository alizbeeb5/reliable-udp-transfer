"""
test_window.py -- Acceptance tests for Part 2 (window.py)
===========================================================

TEAM / AUTHORSHIP
-----------------
Author ........: Ali Hadi Zbeeb   (EECE 350 -- Computer Networks, Group 9)
Part tested ...: Part 2 -- sender sending window, timers and retransmission
File written .: Ali Hadi Zbeeb
AI disclosure : Drafted with the help of an AI coding assistant (MiniMax M3)
                used by Ali Hadi Zbeeb, as required by the course assignment.

WHAT THIS FILE IS
-----------------
The tests below check the behaviour that CONTRACT.md section 10 ("Integration
acceptance checklist") requires from Part 2, using:

  * a scripted channel double that records every submission instead of
    touching a socket, and
  * fake monotonic timestamps, so every timing rule is exact and
    reproducible (contract section 10: "Use scripted channel decisions/fake
    monotonic times for exact behavior checks").

Run with:

    python3 -m unittest -v test_window.py

The tests are self-contained: if Part 1's ``protocol.py`` is not in the
repository yet, a minimal stub is injected so Part 2 can be tested in
isolation.  When the real ``protocol.py`` exists it is always used.
"""

from __future__ import annotations

import os
import sys
import types
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# --- use the real protocol.py when Part 1 has delivered it ------------------
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

from window import (  # noqa: E402
    ACKNOWLEDGED,
    DUP_ACK_THRESHOLD,
    MAX_RTO,
    RETRANSMITTED,
    SENT,
    UNSENT,
    SenderWindow,
)


T0 = 1000.0  # arbitrary monotonic base for the fake clock


def make_packets(count: int):
    """Fake encoded datagrams; the 4-digit index is recoverable from the bytes."""
    return [("PKT%04d" % index).encode("ascii") for index in range(count)]


class RecordingChannel:
    """Stand-in for Part 1's ``UnreliableChannel.send``.

    It records every submission (datagram, timestamp) so the test can assert
    exactly which packet went to the channel, how often, and when.  It never
    touches a socket -- exactly like the real simulator's ``send`` contract
    (returns ``None``, never a delivery promise).
    """

    def __init__(self, packets):
        self.packets = list(packets)
        self.submissions = []  # list of (seq, now)

    def send(self, datagram: bytes, now: float) -> None:
        self.submissions.append((int(datagram[3:7]), now))

    # -- helpers used by the assertions ---------------------------------
    @property
    def seqs(self):
        return [seq for seq, _ in self.submissions]

    def count(self, seq=None):
        if seq is None:
            return len(self.submissions)
        return sum(1 for submitted, _ in self.submissions if submitted == seq)

    def last_time(self):
        return self.submissions[-1][1] if self.submissions else None


class WindowTestCase(unittest.TestCase):
    """Shared fixture: a window plus its recording channel and event log."""

    packet_count = 10
    window_size = 4

    def build(self, packet_count=None, window_size=None, start=T0):
        packet_count = self.packet_count if packet_count is None else packet_count
        window_size = self.window_size if window_size is None else window_size
        self.channel = RecordingChannel(make_packets(packet_count))
        self.events = []
        self.window = SenderWindow(
            self.channel.packets,
            window_size=window_size,
            send_packet=self.channel.send,
            emit=self.events.append,
        )
        self.start = start
        return self.window

    def event_names(self):
        return [event["event"] for event in self.events]

    def events_named(self, name):
        return [event for event in self.events if event["event"] == name]


class TestConstruction(WindowTestCase):
    """Contract section 8: constructor, validation, no sends."""

    def test_constructor_performs_no_send(self):
        window = self.build()
        self.assertEqual(self.channel.count(), 0)
        self.assertFalse(window.done())

    def test_invalid_window_size_raises_value_error(self):
        for bad in (0, -1, 4097, "8", None, 1.5):
            with self.subTest(window_size=bad):
                with self.assertRaises(ValueError):
                    SenderWindow(
                        make_packets(3),
                        window_size=bad,
                        send_packet=lambda datagram, now: None,
                        emit=None,
                    )

    def test_window_size_one_is_allowed(self):
        window = self.build(packet_count=3, window_size=1)
        window.fill_window(T0)
        self.assertEqual(self.channel.seqs, [0])

    def test_empty_packet_list_is_rejected(self):
        with self.assertRaises(ValueError):
            SenderWindow(
                [], window_size=4, send_packet=lambda datagram, now: None, emit=None
            )

    def test_emit_may_be_none(self):
        window = SenderWindow(
            make_packets(2), window_size=2, send_packet=lambda datagram, now: None, emit=None
        )
        window.fill_window(T0)
        window.on_ack(2, T0 + 0.1)
        self.assertTrue(window.done())


class TestFilling(WindowTestCase):
    """Contract section 6: "New sends are allowed while next_seq < min(base + N, P)"."""

    def test_fill_window_sends_exactly_n_packets(self):
        window = self.build(packet_count=10, window_size=4)
        window.fill_window(T0)
        self.assertEqual(self.channel.seqs, [0, 1, 2, 3])

    def test_fill_window_is_idempotent(self):
        window = self.build(packet_count=10, window_size=4)
        window.fill_window(T0)
        window.fill_window(T0 + 1.0)
        window.fill_window(T0 + 2.0)
        self.assertEqual(self.channel.seqs, [0, 1, 2, 3])

    def test_fill_window_stops_at_packet_count(self):
        window = self.build(packet_count=2, window_size=8)
        window.fill_window(T0)
        self.assertEqual(self.channel.seqs, [0, 1])

    def test_on_ack_does_not_fill_the_window(self):
        window = self.build(packet_count=10, window_size=4)
        window.fill_window(T0)
        window.on_ack(2, T0 + 0.1)
        # The ACK moved base to 2 but must not submit anything by itself.
        self.assertEqual(self.channel.seqs, [0, 1, 2, 3])

    def test_window_never_exceeds_n_outstanding(self):
        window = self.build(packet_count=20, window_size=3)
        window.fill_window(T0)
        for ack in range(1, 12):
            window.on_ack(ack, T0 + ack * 0.01)
            window.fill_window(T0 + ack * 0.01)
            snapshot = window.snapshot()
            outstanding = snapshot["next_seq"] - snapshot["base"]
            self.assertLessEqual(outstanding, snapshot["window_size"])
            # The window is exactly full after every refill.
            self.assertEqual(outstanding, snapshot["window_size"])
        # At the end the window is drained and the transfer is complete.
        for ack in range(12, 21):
            window.on_ack(ack, T0 + 1.0)
            window.fill_window(T0 + 1.0)
        self.assertTrue(window.done())
        self.assertEqual(window.snapshot()["next_seq"] - window.snapshot()["base"], 0)

    def test_new_packets_are_sent_after_the_window_slides(self):
        window = self.build(packet_count=10, window_size=4)
        window.fill_window(T0)
        window.on_ack(4, T0 + 0.1)
        window.fill_window(T0 + 0.1)
        self.assertEqual(self.channel.seqs, [0, 1, 2, 3, 4, 5, 6, 7])


class TestAcknowledgement(WindowTestCase):
    """Contract section 6: cumulative ACK handling."""

    def test_advancing_ack_moves_base_and_acknowledges_the_run(self):
        window = self.build(packet_count=10, window_size=4)
        window.fill_window(T0)
        window.on_ack(3, T0 + 0.05)
        snapshot = window.snapshot()
        self.assertEqual(snapshot["base"], 3)
        self.assertEqual(snapshot["next_seq"], 4)
        self.assertEqual(snapshot["dup_ack_count"], 0)
        ack_event = self.events_named("ACK")[0]
        self.assertEqual(ack_event["acked"], [0, 1, 2])

    def test_cumulative_jump_acknowledges_everything_below(self):
        window = self.build(packet_count=10, window_size=8)
        window.fill_window(T0)
        window.on_ack(6, T0 + 0.05)
        snapshot = window.snapshot()
        self.assertEqual(snapshot["base"], 6)
        self.assertEqual(self.events_named("ACK")[0]["acked"], [0, 1, 2, 3, 4, 5])
        # The window may now be refilled without exceeding N.
        window.fill_window(T0 + 0.05)
        self.assertEqual(window.snapshot()["next_seq"], 10)
        self.assertEqual(window.snapshot()["next_seq"] - window.snapshot()["base"], 4)

    def test_stale_ack_is_ignored(self):
        window = self.build(packet_count=10, window_size=4)
        window.fill_window(T0)
        window.on_ack(3, T0 + 0.05)
        window.on_ack(1, T0 + 0.06)
        snapshot = window.snapshot()
        self.assertEqual(snapshot["base"], 3)
        self.assertEqual(snapshot["dup_ack_count"], 0)
        self.assertEqual(self.channel.count(), 4)
        self.assertIn("STALE_ACK", self.event_names())

    def test_ack_for_unsent_data_is_ignored(self):
        window = self.build(packet_count=10, window_size=4)
        window.fill_window(T0)
        window.on_ack(99, T0 + 0.05)
        snapshot = window.snapshot()
        self.assertEqual(snapshot["base"], 0)
        self.assertEqual(snapshot["next_seq"], 4)
        self.assertEqual(self.channel.count(), 4)
        self.assertIn("ACK_UNSENT", self.event_names())

    def test_equal_ack_without_outstanding_data_is_not_a_duplicate(self):
        window = self.build(packet_count=2, window_size=2)
        window.fill_window(T0)
        window.on_ack(2, T0 + 0.05)  # everything acknowledged, window empty
        window.on_ack(2, T0 + 0.06)
        window.on_ack(2, T0 + 0.07)
        snapshot = window.snapshot()
        self.assertEqual(snapshot["dup_ack_count"], 0)
        self.assertEqual(self.channel.count(), 2)

    def test_duplicate_acks_are_counted_but_do_not_move_base(self):
        window = self.build(packet_count=10, window_size=4)
        window.fill_window(T0)
        window.on_ack(0, T0 + 0.05)
        window.on_ack(0, T0 + 0.06)
        snapshot = window.snapshot()
        self.assertEqual(snapshot["base"], 0)
        self.assertEqual(snapshot["dup_ack_count"], 2)
        self.assertEqual(self.channel.count(), 4)  # no retransmission yet

    def test_invalid_acknowledgement_values_are_ignored(self):
        """Extra hardening: Part 1 hands over validated ACK numbers, but the
        window still refuses nonsense instead of corrupting its state."""
        window = self.build(packet_count=10, window_size=4)
        window.fill_window(T0)
        window.on_ack(-1, T0 + 0.05)
        window.on_ack("3", T0 + 0.06)
        snapshot = window.snapshot()
        self.assertEqual(snapshot["base"], 0)
        self.assertEqual(snapshot["next_seq"], 4)
        self.assertIn("ACK_IGNORED", self.event_names())

    def test_two_gaps_in_one_window_are_repaired_one_after_the_other(self):
        """Checklist row: "First gap repaired; cumulative ACK exposes next base;
        timer repairs second gap."

        The receiver gets packets 0, 2 and 4 but never 1 and not yet 3.  The
        sender repairs packet 1 with the timer, learns the new base from the
        cumulative ACK, and then repairs packet 3 with the next timer expiry.
        """
        window = self.build(packet_count=6, window_size=6)
        window.fill_window(T0)
        self.assertEqual(self.channel.seqs, [0, 1, 2, 3, 4, 5])

        # Receiver: packet 0 -> ACK 1; packets 2 and 4 -> ACK 1, ACK 1.
        window.on_ack(1, T0 + 0.01)          # base = 1
        window.on_ack(1, T0 + 0.02)          # duplicate 1
        window.on_ack(1, T0 + 0.03)          # duplicate 2
        self.assertEqual(window.snapshot()["base"], 1)
        self.assertEqual(window.snapshot()["dup_ack_count"], 2)
        self.assertEqual(window.stats()["timeouts"], 0)

        # The timer repairs the first gap: only packet 1 is retransmitted.
        window.on_tick(window.snapshot()["timer_deadline"])
        self.assertEqual(self.channel.seqs[-1], 1)
        self.assertEqual(window.stats()["timeouts"], 1)

        # The repaired packet drains the buffered run: the cumulative ACK jumps
        # over packet 2 and exposes the next gap as the new base.
        window.on_ack(3, T0 + 2.0)
        snapshot = window.snapshot()
        self.assertEqual(snapshot["base"], 3)
        self.assertEqual(snapshot["dup_ack_count"], 0)
        self.assertIsNotNone(snapshot["timer_deadline"])  # timer restarted

        # The second gap (the new base, packet 3) is repaired by the timer.
        window.on_tick(snapshot["timer_deadline"])
        self.assertEqual(self.channel.seqs[-1], 3)
        self.assertEqual(window.stats()["timeouts"], 2)

        # And the transfer continues normally afterwards.
        window.on_ack(6, T0 + 4.0)
        self.assertTrue(window.done())

    def test_stale_ack_does_not_count_as_a_duplicate(self):
        window = self.build(packet_count=10, window_size=4)
        window.fill_window(T0)
        window.on_ack(2, T0 + 0.01)
        window.on_ack(0, T0 + 0.02)   # stale
        window.on_ack(1, T0 + 0.03)   # stale
        window.on_ack(2, T0 + 0.04)   # duplicate of the current base
        snapshot = window.snapshot()
        self.assertEqual(snapshot["dup_ack_count"], 1)
        self.assertEqual(window.stats()["fast_retransmits"], 0)


class TestFastRetransmit(WindowTestCase):
    """Contract section 6: three duplicate ACKs -> one fast retransmit of base."""

    def test_three_duplicate_acks_trigger_one_fast_retransmit(self):
        window = self.build(packet_count=10, window_size=4)
        window.fill_window(T0)
        # Nothing happens before the threshold is reached.
        for index in range(DUP_ACK_THRESHOLD - 1):
            window.on_ack(0, T0 + 0.05 + index * 0.01)
            self.assertEqual(self.channel.count(0), 1)
            self.assertEqual(window.stats()["fast_retransmits"], 0)
        # The threshold ACK triggers the fast retransmission of packet 0.
        window.on_ack(0, T0 + 0.05 + (DUP_ACK_THRESHOLD - 1) * 0.01)
        self.assertEqual(self.channel.count(0), 2)  # packet 0 resent once
        self.assertEqual(self.channel.seqs[-1], 0)
        self.assertEqual(window.stats()["fast_retransmits"], 1)

    def test_repeated_duplicate_acks_do_not_loop(self):
        window = self.build(packet_count=10, window_size=4)
        window.fill_window(T0)
        for index in range(10):
            window.on_ack(0, T0 + 0.05 + index * 0.01)
        self.assertEqual(self.channel.count(0), 2)  # original + one fast retransmit
        self.assertEqual(window.stats()["fast_retransmits"], 1)

    def test_fast_retransmit_is_blocked_until_base_advances(self):
        window = self.build(packet_count=10, window_size=4)
        window.fill_window(T0)
        for index in range(3):
            window.on_ack(0, T0 + 0.05 + index * 0.01)
        self.assertEqual(window.stats()["fast_retransmits"], 1)
        # base advances -> the guard is reset
        window.on_ack(1, T0 + 0.2)
        for index in range(3):
            window.on_ack(1, T0 + 0.21 + index * 0.01)
        self.assertEqual(window.stats()["fast_retransmits"], 2)
        self.assertEqual(self.channel.count(1), 2)

    def test_fast_retransmit_does_not_double_the_rto(self):
        window = self.build(packet_count=10, window_size=4)
        window.fill_window(T0)
        for index in range(3):
            window.on_ack(0, T0 + 0.05 + index * 0.01)
        self.assertEqual(window.snapshot()["rto"], 1.0)

    def test_fast_retransmit_restarts_the_timer(self):
        window = self.build(packet_count=10, window_size=4)
        window.fill_window(T0)
        deadline_before = window.snapshot()["timer_deadline"]
        for index in range(3):
            window.on_ack(0, T0 + 0.05 + index * 0.01)
        deadline_after = window.snapshot()["timer_deadline"]
        self.assertNotEqual(deadline_before, deadline_after)
        self.assertAlmostEqual(deadline_after, T0 + 0.07 + 1.0)
        # The reset prevents an immediate second timeout in the same iteration.
        window.on_tick(T0 + 0.07)
        self.assertEqual(window.stats()["timeouts"], 0)


class TestTimeout(WindowTestCase):
    """Contract section 6: timeout retransmits ONLY base and backs off the RTO."""

    def test_timeout_retransmits_only_the_base_packet(self):
        window = self.build(packet_count=10, window_size=4)
        window.fill_window(T0)
        window.on_tick(T0 + 1.0)
        self.assertEqual(self.channel.seqs, [0, 1, 2, 3, 0])
        self.assertEqual(window.stats()["timeouts"], 1)

    def test_timeout_doubles_the_rto_and_restarts_the_deadline(self):
        window = self.build(packet_count=10, window_size=4)
        window.fill_window(T0)
        window.on_tick(T0 + 1.0)
        snapshot = window.snapshot()
        self.assertEqual(snapshot["rto"], 2.0)
        self.assertAlmostEqual(snapshot["timer_deadline"], T0 + 1.0 + 2.0)

    def test_repeated_timeouts_keep_backing_off_up_to_the_cap(self):
        window = self.build(packet_count=10, window_size=4)
        window.fill_window(T0)
        now = T0 + 1.0
        for expected in (2.0, 4.0, 8.0):
            window.on_tick(now)
            self.assertEqual(window.snapshot()["rto"], expected)
            now = window.snapshot()["timer_deadline"]
        # Push the RTO to the 60 s ceiling.
        for _ in range(10):
            now = window.snapshot()["timer_deadline"]
            window.on_tick(now)
        self.assertEqual(window.snapshot()["rto"], MAX_RTO)

    def test_on_tick_handles_one_expiry_per_call(self):
        window = self.build(packet_count=10, window_size=4)
        window.fill_window(T0)
        window.on_tick(T0 + 100.0)
        self.assertEqual(window.stats()["timeouts"], 1)
        self.assertEqual(self.channel.count(0), 2)

    def test_duplicate_and_stale_acks_cannot_postpone_the_timeout(self):
        window = self.build(packet_count=10, window_size=4)
        window.fill_window(T0)
        deadline = window.snapshot()["timer_deadline"]
        window.on_ack(0, T0 + 0.5)
        window.on_ack(0, T0 + 0.6)
        self.assertAlmostEqual(window.snapshot()["timer_deadline"], deadline)
        window.on_tick(deadline)
        self.assertEqual(window.stats()["timeouts"], 1)

    def test_new_sends_do_not_restart_the_timer(self):
        window = self.build(packet_count=10, window_size=4)
        window.fill_window(T0)
        window.on_ack(2, T0 + 0.1)          # base = 2, packets 2 and 3 still outstanding
        deadline = window.snapshot()["timer_deadline"]
        window.fill_window(T0 + 0.2)        # submits packets 4 and 5
        self.assertEqual(window.snapshot()["timer_deadline"], deadline)
        self.assertEqual(self.channel.seqs, [0, 1, 2, 3, 4, 5])

    def test_advancing_ack_restarts_the_timer(self):
        window = self.build(packet_count=10, window_size=4)
        window.fill_window(T0)
        window.on_ack(2, T0 + 0.5)
        self.assertAlmostEqual(window.snapshot()["timer_deadline"], T0 + 0.5 + 1.0)

    def test_timer_stops_when_nothing_is_outstanding(self):
        window = self.build(packet_count=2, window_size=2)
        window.fill_window(T0)
        window.on_ack(2, T0 + 0.1)
        self.assertIsNone(window.snapshot()["timer_deadline"])
        window.on_tick(T0 + 100.0)
        self.assertEqual(window.stats()["timeouts"], 0)


class TestRttEstimation(WindowTestCase):
    """Contract section 6: RFC 6298 style estimator with conservative sampling."""

    def test_rtt_sample_updates_srtt_rttvar_and_rto(self):
        window = self.build(packet_count=4, window_size=1)
        window.fill_window(T0)
        window.on_ack(1, T0 + 0.2)  # R = 0.2 s
        stats = window.stats()
        self.assertAlmostEqual(stats["srtt"], 0.2)
        self.assertAlmostEqual(stats["rttvar"], 0.1)
        self.assertEqual(stats["rto"], 1.0)  # clamped to the 1 s minimum

        window.fill_window(T0 + 0.2)
        window.on_ack(2, T0 + 0.5)  # R = 0.3 s
        stats = window.stats()
        self.assertAlmostEqual(stats["srtt"], 0.2125)
        self.assertAlmostEqual(stats["rttvar"], 0.1)
        self.assertEqual(stats["rto"], 1.0)

    def test_cumulative_jump_produces_no_sample(self):
        window = self.build(packet_count=8, window_size=4)
        window.fill_window(T0)
        window.on_ack(4, T0 + 0.2)
        self.assertIsNone(window.stats()["srtt"])

    def test_retransmitted_packet_produces_no_sample(self):
        window = self.build(packet_count=8, window_size=4)
        window.fill_window(T0)
        window.on_tick(T0 + 1.0)  # packet 0 is retransmitted
        window.on_ack(1, T0 + 1.2)  # advances by one, but attempts == 2
        self.assertIsNone(window.stats()["srtt"])

    def test_backoff_is_only_cleared_by_a_new_eligible_sample(self):
        window = self.build(packet_count=4, window_size=1)
        window.fill_window(T0)
        window.on_tick(T0 + 1.0)
        self.assertEqual(window.snapshot()["rto"], 2.0)
        # An ACK that does not qualify as a sample leaves the backoff alone.
        window.on_ack(1, T0 + 1.1)
        self.assertEqual(window.snapshot()["rto"], 2.0)


class TestCompletion(WindowTestCase):
    """Contract section 6: completion is base == P, not "everything was sent"."""

    def test_not_done_while_packets_are_unacknowledged(self):
        window = self.build(packet_count=10, window_size=4)
        window.fill_window(T0)
        self.assertFalse(window.done())

    def test_done_only_after_the_end_packet_is_acknowledged(self):
        window = self.build(packet_count=3, window_size=2)
        window.fill_window(T0)              # sends DATA 0 and DATA 1
        window.on_ack(2, T0 + 0.1)          # DATA 0 and 1 acknowledged
        self.assertFalse(window.done())    # END (seq 2) has not even been sent yet
        window.fill_window(T0 + 0.1)        # the sender submits END
        self.assertEqual(self.channel.seqs[-1], 2)
        self.assertFalse(window.done())    # END is not acknowledged yet
        window.on_ack(3, T0 + 0.2)
        self.assertTrue(window.done())

    def test_end_packet_is_retransmitted_like_any_other_packet(self):
        window = self.build(packet_count=3, window_size=2)
        window.fill_window(T0)
        window.on_ack(2, T0 + 0.1)  # base = 2, END is the only outstanding packet
        window.fill_window(T0 + 0.1)
        self.assertEqual(self.channel.seqs[-1], 2)
        window.on_tick(window.snapshot()["timer_deadline"])
        self.assertEqual(self.channel.seqs[-1], 2)
        self.assertEqual(window.stats()["retransmissions"], 1)

    def test_transfer_time_starts_at_the_first_submission(self):
        window = self.build(packet_count=3, window_size=3)
        window.fill_window(T0)
        window.on_ack(3, T0 + 2.5)
        self.assertAlmostEqual(window.stats()["transfer_time_s"], 2.5)

    def test_transfer_time_while_active(self):
        window = self.build(packet_count=10, window_size=4)
        window.fill_window(T0)
        window.on_tick(T0 + 1.0)
        self.assertAlmostEqual(window.stats(now=T0 + 3.0)["transfer_time_s"], 3.0)


class TestStatistics(WindowTestCase):
    """Contract section 9: counter meanings and identities."""

    def test_counter_identities_hold(self):
        packet_count = 10
        window = self.build(packet_count=packet_count, window_size=4)
        window.fill_window(T0)
        # one fast retransmit
        for index in range(3):
            window.on_ack(0, T0 + 0.05 + index * 0.01)
        # one timeout
        window.on_tick(window.snapshot()["timer_deadline"])
        # and a normal completion
        window.on_ack(1, T0 + 5.0)
        for ack in range(2, packet_count + 1):
            window.on_ack(ack, T0 + 5.0 + ack * 0.01)
            window.fill_window(T0 + 5.0 + ack * 0.01)

        stats = window.stats()
        self.assertTrue(window.done())
        self.assertEqual(stats["retransmissions"], stats["timeouts"] + stats["fast_retransmits"])
        # P is the number of prepared packets (setup owns it), not a window key.
        self.assertEqual(stats["transmissions"], packet_count + stats["retransmissions"])
        self.assertEqual(stats["acked_packets"], packet_count)
        self.assertEqual(stats["total_packets"], packet_count)

    def test_transmissions_count_every_submission(self):
        window = self.build(packet_count=4, window_size=2)
        window.fill_window(T0)
        self.assertEqual(window.stats()["transmissions"], 2)
        window.on_ack(1, T0 + 0.1)
        window.fill_window(T0 + 0.1)
        self.assertEqual(window.stats()["transmissions"], 3)

    def test_acked_packets_is_base(self):
        window = self.build(packet_count=10, window_size=4)
        window.fill_window(T0)
        window.on_ack(3, T0 + 0.1)
        self.assertEqual(window.stats()["acked_packets"], 3)


class TestSnapshot(WindowTestCase):
    """Contract section 8: snapshot contents and copy semantics."""

    def test_snapshot_contains_the_contract_keys(self):
        window = self.build(packet_count=10, window_size=4)
        window.fill_window(T0)
        snapshot = window.snapshot()
        for key in (
            "base",
            "next_seq",
            "window_size",
            "rto",
            "timer_deadline",
            "dup_ack_count",
            "packets",
        ):
            self.assertIn(key, snapshot)

    def test_snapshot_packet_states(self):
        window = self.build(packet_count=10, window_size=4)
        window.fill_window(T0)
        snapshot = window.snapshot()
        self.assertEqual([slot["seq"] for slot in snapshot["packets"]], [0, 1, 2, 3])
        for slot in snapshot["packets"]:
            self.assertEqual(slot["state"], SENT)
            self.assertEqual(slot["attempts"], 1)

    def test_snapshot_shows_unsent_slots(self):
        window = self.build(packet_count=10, window_size=4)
        window.fill_window(T0)
        window.on_ack(4, T0 + 0.1)         # base = 4, nothing outstanding any more
        snapshot = window.snapshot()
        self.assertEqual([slot["seq"] for slot in snapshot["packets"]], [4, 5, 6, 7])
        for slot in snapshot["packets"]:
            self.assertEqual(slot["state"], UNSENT)
            self.assertEqual(slot["attempts"], 0)
        # After the refill the same slots are SENT.
        window.fill_window(T0 + 0.1)
        snapshot = window.snapshot()
        for slot in snapshot["packets"]:
            self.assertEqual(slot["state"], SENT)
            self.assertEqual(slot["attempts"], 1)

    def test_snapshot_shows_retransmitted_state(self):
        window = self.build(packet_count=10, window_size=4)
        window.fill_window(T0)
        window.on_tick(T0 + 1.0)
        snapshot = window.snapshot()
        self.assertEqual(snapshot["packets"][0]["state"], RETRANSMITTED)
        self.assertEqual(snapshot["packets"][0]["attempts"], 2)

    def test_snapshot_shows_acknowledged_state_in_the_history(self):
        """Extension test (not a contract requirement): the contract's
        ``packets`` list holds the CURRENT window slots, which are by
        definition unacknowledged.  ``acked_history`` is the extra key that
        lets the renderer show the ACKNOWLEDGED state as the window slides."""
        window = self.build(packet_count=10, window_size=4)
        window.fill_window(T0)
        window.on_ack(2, T0 + 0.1)
        history = window.snapshot()["acked_history"]
        self.assertEqual([slot["seq"] for slot in history], [0, 1])
        for slot in history:
            self.assertEqual(slot["state"], ACKNOWLEDGED)

    def test_snapshot_is_a_copy(self):
        window = self.build(packet_count=10, window_size=4)
        window.fill_window(T0)
        snapshot = window.snapshot()
        snapshot["base"] = 999
        snapshot["packets"][0]["state"] = "TAMPERED"
        snapshot["packets"].clear()
        fresh = window.snapshot()
        self.assertEqual(fresh["base"], 0)
        self.assertEqual(fresh["packets"][0]["state"], SENT)

    def test_stats_is_a_copy(self):
        window = self.build(packet_count=10, window_size=4)
        window.fill_window(T0)
        stats = window.stats()
        stats["transmissions"] = 999
        self.assertEqual(window.stats()["transmissions"], 4)


class TestEvents(WindowTestCase):
    """Contract section 8: the shared event log format."""

    def test_every_event_has_time_side_and_event(self):
        window = self.build(packet_count=10, window_size=4)
        window.fill_window(T0)
        window.on_ack(1, T0 + 0.1)
        window.on_tick(T0 + 5.0)
        self.assertTrue(self.events)
        for event in self.events:
            self.assertIn("time", event)
            self.assertIn("side", event)
            self.assertIn("event", event)
            self.assertEqual(event["side"], "sender")
            self.assertIsInstance(event["time"], float)

    def test_expected_event_sequence(self):
        window = self.build(packet_count=10, window_size=4)
        window.fill_window(T0)
        for index in range(3):
            window.on_ack(0, T0 + 0.05 + index * 0.01)
        names = self.event_names()
        self.assertIn("SEND", names)
        self.assertIn("DUP_ACK", names)
        self.assertIn("RETRANSMIT", names)
        self.assertIn("FAST_RETRANSMIT", names)
        self.assertIn("WINDOW", names)
        # The order matters for the live display: the packets are sent first,
        # then the duplicate ACKs arrive, then the fast retransmit follows.
        self.assertLess(names.index("SEND"), names.index("DUP_ACK"))
        self.assertLess(names.index("DUP_ACK"), names.index("RETRANSMIT"))
        self.assertLess(names.index("RETRANSMIT"), names.index("FAST_RETRANSMIT"))
        # A timeout is reported before the retransmission it causes.
        window.on_tick(window.snapshot()["timer_deadline"] + 10.0)
        names = self.event_names()
        self.assertLess(names.index("TIMEOUT"), len(names) - 1)
        self.assertEqual(names[-1], "RTO_BACKOFF")
        self.assertEqual(names[-3], "TIMEOUT")
        self.assertEqual(names[-2], "RETRANSMIT")

    def test_timer_events_carry_the_rto(self):
        window = self.build(packet_count=10, window_size=4)
        window.fill_window(T0)
        window.on_tick(T0 + 1.0)
        timeout_event = self.events_named("TIMEOUT")[0]
        self.assertIn("rto", timeout_event)
        self.assertEqual(timeout_event["seq"], 0)

    def test_send_events_carry_the_packet_state(self):
        window = self.build(packet_count=10, window_size=4)
        window.fill_window(T0)
        send_event = self.events_named("SEND")[0]
        self.assertEqual(send_event["seq"], 0)
        self.assertEqual(send_event["attempt"], 1)
        self.assertEqual(send_event["packet"]["state"], SENT)

    def test_window_events_carry_the_packet_states(self):
        window = self.build(packet_count=10, window_size=4)
        window.fill_window(T0)
        window_event = self.events_named("WINDOW")[0]
        self.assertIn("packets", window_event)
        self.assertEqual(len(window_event["packets"]), 4)


class TestEndToEndScenario(WindowTestCase):
    """A complete lossy run driven only through the public interface."""

    def test_loss_recovery_completes_the_transfer(self):
        """Full lossy run: DATA 0 is lost once (fast retransmit), END is lost
        once (timeout retransmit).  The transfer must still complete and all
        counter identities from contract section 9 must hold."""
        packet_count = 12
        window = self.build(packet_count=packet_count, window_size=4)
        window.fill_window(T0)

        now = T0
        seen_submissions = 0
        received = set()
        acked = 0
        lost_once = {0, packet_count - 1}   # DATA 0 and END lose their first attempt
        lost_already = set()
        guard = 0

        while not window.done() and guard < 1000:
            guard += 1
            now += 0.05

            # Deliver every submission the channel accepted since the last round.
            # The receiver sends ONE cumulative ACK per received datagram
            # (contract section 4), so losing DATA 0 while 1, 2 and 3 arrive
            # produces ACK 0, 0, 0 -> one fast retransmit of packet 0.
            while seen_submissions < len(self.channel.submissions):
                seq, _ = self.channel.submissions[seen_submissions]
                seen_submissions += 1
                if seq in lost_once and seq not in lost_already:
                    lost_already.add(seq)       # this attempt was lost in the network
                    continue
                received.add(seq)
                while acked in received:
                    acked += 1
                window.on_ack(acked, now)

            window.on_tick(now)
            window.fill_window(now)

        self.assertTrue(window.done())
        stats = window.stats()
        self.assertEqual(stats["acked_packets"], packet_count)
        self.assertEqual(stats["transmissions"], packet_count + stats["retransmissions"])
        self.assertEqual(stats["retransmissions"], stats["timeouts"] + stats["fast_retransmits"])
        self.assertGreaterEqual(stats["fast_retransmits"], 1)
        self.assertGreaterEqual(stats["timeouts"], 1)
        # The lost DATA packet and the lost END packet were both resent.
        self.assertGreaterEqual(self.channel.count(0), 2)
        self.assertGreaterEqual(self.channel.count(packet_count - 1), 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
