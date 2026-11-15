# Member 1: Ghadi Zouki

import random

from protocol import decode_packet


class UnreliableChannel:
    def __init__(self, sock, peer, *, loss, reorder, seed, emit):
        if not 0 <= loss <= 1 or not 0 <= reorder <= 1:
            raise ValueError("Loss and reorder probabilities must be between 0 and 1")

        self.socket = sock
        self.receiver_address = peer
        self.loss_probability = loss
        self.reorder_probability = reorder
        self.random_generator = random.Random(seed)
        self.emit_event = emit

        self.waiting_packet = None
        self.hold_deadline = None

        self.counts = {
            "submitted": 0,
            "dropped": 0,
            "held": 0,
            "reordered": 0,
            "flushed": 0,
            "udp_sent": 0,
        }

    def send(self, datagram, now):
        self.tick(now)
        self.counts["submitted"] += 1

        if self.random_generator.random() < self.loss_probability:
            self.counts["dropped"] += 1
            self._log("DROP", now, datagram)
            return

        if self.waiting_packet is not None:
            self._send_udp(datagram)
            self._log("SEND", now, datagram)

            earlier_packet = self.waiting_packet
            self._send_udp(earlier_packet)

            self.waiting_packet = None
            self.hold_deadline = None
            self.counts["reordered"] += 1

            later_sequence = decode_packet(datagram).seq
            self._log(
                "REORDERED", now, earlier_packet,
                after_seq=later_sequence,
            )
            return

        if self.random_generator.random() < self.reorder_probability:
            self.waiting_packet = datagram
            self.hold_deadline = now + 0.1
            self.counts["held"] += 1
            self._log("HOLD", now, datagram)
            return

        self._send_udp(datagram)
        self._log("SEND", now, datagram)

    def tick(self, now):
        if self.waiting_packet is None:
            return
        if now < self.hold_deadline:
            return

        released_packet = self.waiting_packet
        self._send_udp(released_packet)

        self.waiting_packet = None
        self.hold_deadline = None
        self.counts["flushed"] += 1
        self._log("FLUSH", now, released_packet)

    def has_pending(self):
        return self.waiting_packet is not None

    def stats(self):
        current_counts = self.counts.copy()
        current_counts["pending"] = int(self.has_pending())
        return current_counts

    def _send_udp(self, datagram):
        self.socket.sendto(datagram, self.receiver_address)
        self.counts["udp_sent"] += 1

    def _log(self, event_name, now, datagram, **details):
        message = {
            "time": now,
            "side": "channel",
            "event": event_name,
            "seq": decode_packet(datagram).seq,
        }
        message.update(details)
        self.emit_event(message)