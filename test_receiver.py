# Test script for receiver.py (Member 3: Yara El Rohayem)
# AI assistance: written by Claude (Anthropic) to test the receiver alone.
# Run receiver.py first, then run this file in a second terminal.

import hashlib
import socket

from protocol import DATA, END, encode_packet, decode_packet

RECEIVER = ("127.0.0.1", 9000)

data = b"Hello EECE 350! " * 300          # a small fake file (4800 bytes)
chunks = [data[i:i + 1024] for i in range(0, len(data), 1024)]
packets = {i: encode_packet(DATA, i, c) for i, c in enumerate(chunks)}
end_seq = len(chunks)
packets[end_seq] = encode_packet(END, end_seq, hashlib.sha256(data).digest())

order = [0, 2, 3, 1, 5, 4, 4]               # gap, END early, END sent again

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.settimeout(2)

acks = []
for seq in order:
    sock.sendto(packets[seq], RECEIVER)
    reply, _ = sock.recvfrom(100)
    acks.append(decode_packet(reply).seq)

print("Sent packets:", order)
print("Got ACKs:   ", acks)
print("Expected:    [1, 1, 1, 4, 4, 6, 6]")
print("TEST PASSED" if acks == [1, 1, 1, 4, 4, 6, 6] else "TEST FAILED")
