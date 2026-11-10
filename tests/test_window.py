# Person 2 tests for Ali Hadi Zbeeb; generated with OpenAI Codex assistance.
import unittest
from unittest.mock import patch

from window import SenderWindow
from tests.harness import Harness


class WindowTests(unittest.TestCase):
    def make(self, count=7, size=4, send=None):
        self.sent = []
        self.events = []
        self.packets = tuple(str(i).encode() for i in range(count))
        self.w = SenderWindow(self.packets, window_size=size,
                              send_packet=send or (lambda p, t: self.sent.append((p, t))),
                              emit=self.events.append)
        return self.w

    def test_invalid_configuration(self):
        for size in (0, -1, 4097, 1.5, True, "4"):
            with self.subTest(size=size), self.assertRaises(ValueError):
                self.make(size=size)
        for packets in ([], b"abc", [bytearray(b"x")], [b""], [1], None):
            with self.subTest(packets=packets), self.assertRaises(ValueError):
                SenderWindow(packets, window_size=1, send_packet=lambda p, t: None,
                             emit=lambda e: None)
        with self.assertRaises(ValueError):
            SenderWindow([b"end"], window_size=1, send_packet=None, emit=print)

    def test_constructor_and_repeat_fill_do_not_send_extra(self):
        w = self.make()
        self.assertEqual(self.sent, [])
        w.fill_window(0)
        w.fill_window(0.5)
        self.assertEqual(len(self.sent), 4)
        self.assertEqual(w.timer_deadline, 1)
        self.assertEqual(w.snapshot()["packets"][0]["state"], "SENT")

    def test_cumulative_ack_slides_without_automatic_fill(self):
        w = self.make()
        w.fill_window(0)
        w.on_ack(3, 0.2)
        self.assertEqual((w.base, w.next_seq), (3, 4))
        self.assertEqual(w.stats()["acked_packets"], 3)
        self.assertIsNone(w.srtt)  # cumulative jumps are not RTT samples
        self.assertEqual([p["state"] for p in w.snapshot()["packets"]],
                         ["SENT", "UNSENT", "UNSENT", "UNSENT"])
        w.fill_window(0.2)
        self.assertEqual((w.base, w.next_seq), (3, 7))

    def test_state_is_recorded_before_channel_callback(self):
        observations = []
        w = self.make(send=lambda p, t: observations.append(
            (self.w.next_seq, self.w.snapshot()["packets"][0]["attempts"])))
        w.fill_window(0)
        self.assertEqual(observations[0], (1, 1))

    def test_three_ack_zero_fast_retransmits_once(self):
        w = self.make()
        w.fill_window(0)
        for now in (0.1, 0.2):
            w.on_ack(0, now)
        self.assertEqual(len(self.sent), 4)
        w.on_ack(0, 0.3)
        self.assertEqual(self.sent[-1], (b"0", 0.3))
        self.assertEqual(w.timer_deadline, 1.3)
        self.assertEqual(w.rto, 1)
        for now in (0.4, 0.5, 0.6, 0.7):
            w.on_ack(0, now)
        self.assertEqual(w.stats()["fast_retransmits"], 1)
        self.assertEqual(w.snapshot()["packets"][0]["state"], "RETRANSMITTED")
        w.on_tick(1.3)
        self.assertEqual(w.stats()["timeouts"], 1)

    def test_fast_retransmit_guard_resets_when_base_advances(self):
        w = self.make()
        w.fill_window(0)
        for _ in range(3):
            w.on_ack(0, 0.1)
        w.on_ack(1, 0.2)
        self.assertEqual(w.dup_ack_count, 0)
        for _ in range(3):
            w.on_ack(1, 0.3)
        self.assertEqual(w.stats()["fast_retransmits"], 2)
        self.assertEqual(self.sent[-1][0], b"1")

    def test_stale_unsent_and_invalid_ack_leave_timer_and_duplicates(self):
        w = self.make()
        w.fill_window(0)
        w.on_ack(1, 0.1)
        w.on_ack(1, 0.2)
        before = w.snapshot()
        for ack in (0, -1, 5, 100, None, "1", True, 1.5):
            w.on_ack(ack, 0.3)
            self.assertEqual(w.snapshot(), before)

    def test_equal_ack_when_idle_is_not_duplicate(self):
        w = self.make(count=2, size=1)
        w.on_ack(0, 0)
        w.fill_window(0)
        w.on_ack(1, 0.1)
        for _ in range(4):
            w.on_ack(1, 0.2)
        self.assertEqual(w.dup_ack_count, 0)
        self.assertIsNone(w.timer_deadline)

    def test_timeout_only_base_backoff_and_cap(self):
        w = self.make()
        w.fill_window(0)
        w.on_tick(0.999)
        self.assertEqual(len(self.sent), 4)
        for expected in (2, 4, 8, 16, 32, 60, 60):
            now = w.timer_deadline
            w.on_tick(now)
            self.assertEqual(w.rto, expected)
            self.assertEqual(w.timer_deadline, now + expected)
            self.assertEqual(self.sent[-1][0], b"0")
            count = len(self.sent)
            w.on_tick(now)
            self.assertEqual(len(self.sent), count)
        self.assertEqual(w.stats()["timeouts"], 7)
        self.assertEqual(w.stats()["retransmissions"], 7)

    def test_duplicate_acks_do_not_postpone_timer(self):
        w = self.make()
        w.fill_window(0)
        w.on_ack(0, 0.8)
        w.on_ack(0, 0.9)
        self.assertEqual(w.timer_deadline, 1)
        w.on_tick(1)
        self.assertEqual(w.stats()["timeouts"], 1)

    def test_fast_retransmit_at_expiry_prevents_second_timeout(self):
        w = self.make()
        w.fill_window(0)
        for now in (0.1, 0.2, 1):
            w.on_ack(0, now)
        w.on_tick(1)
        self.assertEqual(w.stats()["retransmissions"], 1)
        self.assertEqual(w.stats()["timeouts"], 0)

    def test_rtt_formula_uses_old_srtt(self):
        w = self.make(size=1)
        w.fill_window(0)
        w.on_ack(1, 0.2)
        self.assertAlmostEqual(w.srtt, 0.2)
        self.assertAlmostEqual(w.rttvar, 0.1)
        w.fill_window(0.2)
        w.on_ack(2, 0.5)
        self.assertAlmostEqual(w.srtt, 0.2125)
        self.assertAlmostEqual(w.rttvar, 0.1)
        self.assertEqual(w.rto, 1)

    def test_rtt_estimate_can_exceed_minimum(self):
        w = self.make(size=1)
        w.fill_window(0)
        w.on_ack(1, 0.8)
        self.assertAlmostEqual(w.rto, 2.4)

    def test_karn_preserves_backoff_until_clean_sample(self):
        w = self.make(size=1)
        w.fill_window(0)
        w.on_tick(1)
        w.on_ack(1, 1.2)
        self.assertIsNone(w.srtt)
        self.assertEqual(w.rto, 2)
        w.fill_window(1.2)
        w.on_ack(2, 1.4)
        self.assertAlmostEqual(w.srtt, 0.2)
        self.assertEqual(w.rto, 1)

    def test_advancing_ack_restarts_timer_and_new_send_does_not(self):
        w = self.make()
        w.fill_window(0)
        w.on_ack(2, 0.3)
        self.assertEqual(w.timer_deadline, 1.3)
        w.fill_window(0.7)
        self.assertEqual(w.timer_deadline, 1.3)

    def test_ack_before_expiry_processing_avoids_unnecessary_retry(self):
        w = self.make(count=1, size=1)
        w.fill_window(0)
        w.on_ack(1, 1)
        w.on_tick(1)
        self.assertEqual(w.stats()["retransmissions"], 0)
        self.assertTrue(w.done())

    def test_end_requires_ack_and_duration_freezes(self):
        w = self.make(count=1, size=1)
        w.fill_window(10)
        self.assertFalse(w.done())
        with patch("window.time.monotonic", return_value=10.5):
            self.assertEqual(w.stats()["transfer_time_s"], 0.5)
        w.on_tick(11)
        w.on_ack(1, 11.2)
        w.on_tick(100)
        w.fill_window(100)
        w.on_ack(1, 100)
        self.assertTrue(w.done())
        self.assertAlmostEqual(w.stats()["transfer_time_s"], 1.2)
        self.assertIsNone(w.timer_deadline)
        self.assertEqual(w.stats()["transmissions"], 2)
        self.assertNotIn("MATCH", self.events[-1].get("message", ""))

    def test_display_and_statistics_are_copies(self):
        w = self.make()
        self.assertEqual(w.stats()["transfer_time_s"], 0)
        w.fill_window(0)
        snapshot = w.snapshot()
        snapshot["packets"][0]["attempts"] = 99
        snapshot["base"] = 99
        self.events[-1]["packets"].clear()
        stats = w.stats()
        stats["transmissions"] = 99
        self.assertEqual(w.snapshot()["packets"][0]["attempts"], 1)
        self.assertEqual(w.stats()["transmissions"], 4)
        self.assertEqual(w.base, 0)

    def test_channel_exception_is_fatal_not_completion(self):
        def fail(p, t):
            raise OSError("socket failure")
        w = self.make(send=fail)
        with self.assertRaises(OSError):
            w.fill_window(0)
        self.assertFalse(w.done())


class TransferScenarioTests(unittest.TestCase):
    def check(self, harness):
        h = harness.run()
        self.assertEqual(bytes(h.output), h.data)
        self.assertEqual(h.verification, "MATCH")
        self.assertEqual(h.verification_count, 1)
        s = h.window.stats()
        self.assertEqual(s["transmissions"], len(h.packets) + s["retransmissions"])
        self.assertEqual(s["retransmissions"], s["timeouts"] + s["fast_retransmits"])
        self.assertEqual(h.channel_counts["submitted"], s["transmissions"])
        self.assertEqual(s["acked_packets"], len(h.packets))
        return h

    def test_no_loss_binary_and_boundary_file_sizes(self):
        for size in (0, 1, 1024, 1025, 10000):
            for window in (1, 4, 4096):
                with self.subTest(size=size, window=window):
                    data = bytes(i % 256 for i in range(size))
                    h = self.check(Harness(data, window_size=window))
                    self.assertEqual(h.window.stats()["retransmissions"], 0)

    def test_first_packet_loss_fast_retransmits(self):
        h = self.check(Harness(b"x" * 6000, drops={(0, 1)}))
        self.assertEqual(h.window.stats()["fast_retransmits"], 1)
        self.assertEqual(h.window.stats()["timeouts"], 0)

    def test_two_gaps_timeout_repairs_next_base(self):
        h = self.check(Harness(b"x" * 5120, window_size=6, drops={(0, 1), (4, 1)}))
        self.assertEqual(h.window.stats()["fast_retransmits"], 1)
        self.assertEqual(h.window.stats()["timeouts"], 1)
        self.assertTrue(h.window.done())

    def test_end_loss(self):
        h = self.check(Harness(b"abc", drops={(1, 1)}))
        self.assertEqual(h.attempts[1], 2)
        self.assertEqual(h.window.stats()["timeouts"], 1)

    def test_lost_final_ack_retries_end_without_rewriting(self):
        h = self.check(Harness(b"abc", window_size=1, drop_final_ack=True))
        self.assertEqual(h.attempts[1], 2)
        self.assertEqual(h.verification_count, 1)

    def test_empty_file_end_loss_and_final_ack_loss(self):
        h = self.check(Harness(b"", drops={(0, 1)}, drop_final_ack=True))
        self.assertEqual(h.attempts[0], 3)

    def test_reorder_and_window_one_idle_flush(self):
        for size in (0, 6000):
            for window in (1, 4):
                with self.subTest(size=size, window=window):
                    h = self.check(Harness(b"a" * size, window_size=window,
                                           reorder_first=True))
                    self.assertEqual(h.window.stats()["retransmissions"], 0)

    def test_end_buffered_until_missing_data_arrives(self):
        h = self.check(Harness(b"abc", drops={(0, 1)}))
        self.assertEqual(h.attempts[1], 1)  # END need not be retransmitted.
        recv = [e for e in h.events if e["event"] == "RECV"]
        self.assertEqual(recv[0]["seq"], 1)
        self.assertEqual(recv[0]["ack_next"], 0)
        self.assertEqual(recv[-1]["ack_next"], 2)

    def test_seeded_loss_scenarios(self):
        for seed in range(40):
            with self.subTest(seed=seed):
                self.check(Harness(bytes(range(256)) * 32, window_size=1 + seed % 8,
                                   loss=0.3, seed=seed, reorder_first=True))

    def test_total_loss_never_reports_completion(self):
        h = Harness(b"abc", loss=1)
        h.window.fill_window(0)
        for _ in range(10):
            h.window.on_tick(h.window.timer_deadline)
        self.assertFalse(h.window.done())
        self.assertEqual(h.window.base, 0)
        self.assertEqual(h.window.rto, 60)


if __name__ == "__main__":
    unittest.main()
