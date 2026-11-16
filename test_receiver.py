# Test script for receiver.py (Member 3: Yara El Rohayem)
# Run receiver.py first, then run this file in a second terminal.

import hashlib
import socket

from protocol import ACK, CHUNK_SIZE, DATA, END, decode_packet, encode_packet

RECEIVER = ("127.0.0.1", 9000)


def main():
    data = b"Hello EECE 350! " * 300
    chunks = [data[i:i + CHUNK_SIZE] for i in range(0, len(data), CHUNK_SIZE)]
    packets = [encode_packet(DATA, i, chunk) for i, chunk in enumerate(chunks)]
    packets.append(encode_packet(END, len(chunks), hashlib.sha256(data).digest()))


    order = [0, 2, 3, 1, 5, 4, 5]
    expected = [1, 1, 1, 4, 4, 6, 6]
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.settimeout(2)
        acks = []
        for seq in order:
            sock.sendto(packets[seq], RECEIVER)
            reply, address = sock.recvfrom(65535)
            packet = decode_packet(reply)
            if address != RECEIVER or packet.packet_type != ACK:
                raise RuntimeError("Response was not an ACK from the receiver")
            acks.append(packet.seq)

    print("Sent packets:", order)
    print("Got ACKs:   ", acks)
    print("Expected:   ", expected)
    print("TEST PASSED" if acks == expected else "TEST FAILED")
    return 0 if acks == expected else 1


if __name__ == "__main__":
    raise SystemExit(main())
