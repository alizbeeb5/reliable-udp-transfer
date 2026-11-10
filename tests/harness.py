# Person 2 test support for Ali Hadi Zbeeb; generated with OpenAI Codex assistance.
"""In-memory reference peers, NOT the teammates' production implementation.

Use contract-shaped bytes and cumulative ACKs to exercise the real SenderWindow.
The artificial clock makes packet loss and 60-second backoffs test instantly.
"""
from collections import deque
import hashlib
import random
import struct

from window import SenderWindow

HEADER = struct.Struct("!BIH")


def prepare(data):
    chunks = [data[i:i + 1024] for i in range(0, len(data), 1024)]
    packets = [HEADER.pack(1, seq, len(chunk)) + chunk
               for seq, chunk in enumerate(chunks)]
    packets.append(HEADER.pack(3, len(chunks), 32) + hashlib.sha256(data).digest())
    return tuple(packets)


class Harness:
    def __init__(self, data, *, window_size=4, drops=(), reorder_first=False,
                 drop_final_ack=False, loss=0.0, seed=0, emit=None):
        self.data = data
        self.packets = prepare(data)
        self.now = 0.0
        self.queue = deque()
        self.pending = None
        self.expected = 0
        self.buffer = {}
        self.output = bytearray()
        self.verification = "PENDING"
        self.verification_count = 0
        self.attempts = {}
        self.drops = set(drops)
        self.reorder_first = reorder_first
        self.drop_final_ack = drop_final_ack
        self.loss = loss
        self.random = random.Random(seed)
        self.events = []
        self.listener = emit
        self.channel_counts = dict(submitted=0, dropped=0, held=0,
                                   reordered=0, flushed=0, delivered=0)
        self.window = SenderWindow(self.packets, window_size=window_size,
                                   send_packet=self.send, emit=self.emit)

    def emit(self, event):
        self.events.append(event)
        if self.listener:
            self.listener(event)

    def event(self, side, event, **fields):
        self.emit(dict(time=self.now, side=side, event=event, **fields))

    def send(self, packet, now):
        self.now = now
        seq = HEADER.unpack_from(packet)[1]
        self.channel_counts["submitted"] += 1
        self.attempts[seq] = self.attempts.get(seq, 0) + 1
        if (seq, self.attempts[seq]) in self.drops or self.random.random() < self.loss:
            self.channel_counts["dropped"] += 1
            self.event("channel", "DROP", seq=seq)
            return
        if self.pending:
            self.queue.extend([packet, self.pending[0]])
            held_seq = HEADER.unpack_from(self.pending[0])[1]
            self.pending = None
            self.channel_counts["reordered"] += 1
            self.event("channel", "REORDERED", seq=held_seq, after=seq)
        elif self.reorder_first:
            self.reorder_first = False
            self.pending = (packet, now + 0.1)
            self.channel_counts["held"] += 1
            self.event("channel", "HOLD", seq=seq)
        else:
            self.queue.append(packet)

    def receive(self, packet):
        kind, seq, length = HEADER.unpack_from(packet)
        payload = packet[HEADER.size:]
        assert length == len(payload)
        self.channel_counts["delivered"] += 1
        if seq >= self.expected and self.verification == "PENDING":
            self.buffer.setdefault(seq, (kind, payload))
        disposition = ("duplicate" if seq < self.expected else
                       "in_order" if seq == self.expected else "buffered")
        while self.expected in self.buffer and self.verification == "PENDING":
            kind, payload = self.buffer.pop(self.expected)
            self.expected += 1
            if kind == 1:
                self.output.extend(payload)
            else:
                self.verification_count += 1
                self.verification = ("MATCH" if hashlib.sha256(self.output).digest()
                                     == payload else "MISMATCH")
        self.event("receiver", "RECV", seq=seq, reason=disposition,
                   ack_next=self.expected)
        if self.drop_final_ack and self.expected == len(self.packets):
            self.drop_final_ack = False
            self.event("receiver", "DROP_FINAL_ACK", ack_next=self.expected)
            return
        self.window.on_ack(self.expected, self.now)

    def run(self):
        self.window.fill_window(self.now)
        for _ in range(100000):
            self.now = round(self.now + 0.05, 8)
            # A bounded batch: callbacks never synchronously call window.on_ack.
            for _ in range(len(self.queue)):
                self.receive(self.queue.popleft())
            if self.pending and self.now >= self.pending[1]:
                self.queue.append(self.pending[0])
                self.pending = None
                self.channel_counts["flushed"] += 1
                self.event("channel", "FLUSH")
            self.window.on_tick(self.now)
            self.window.fill_window(self.now)
            assert 0 <= self.window.base <= self.window.next_seq <= len(self.packets)
            assert self.window.next_seq - self.window.base <= self.window.window_size
            if self.window.done() and not self.queue and not self.pending:
                return self
        raise AssertionError("test harness did not finish within its simulation budget")
