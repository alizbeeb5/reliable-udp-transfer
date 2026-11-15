# Reliable UDP File Transfer

EECE 350, Group 9. The sender transfers one binary file over UDP using a sliding
window, cumulative ACKs, fast retransmit, and a retransmission timer. The
receiver writes the file and verifies its SHA-256 digest. Python 3.9+ is enough.

## Live demo

Run these commands from the repository root. Start the receiver in terminal 1.

```sh
python3 -u receiver.py --output demo-received.bin
```

Generate the input and start the sender in terminal 2.

```sh
python3 -c "from pathlib import Path; Path('demo.bin').write_bytes(bytes(range(256)) * 64)"
python3 -u sender.py demo.bin --window 8 --loss 0.2 --reorder 0.25 --seed 1
cmp demo.bin demo-received.bin
```

This run sends 16 DATA packets plus END. Watch the sender for drops,
reordering, three duplicate ACKs followed by a fast retransmit, and a timeout.
The receiver prints `File check: MATCH` when the written bytes have the
expected SHA-256. The sender reports `TRANSPORT COMPLETE` after all packets,
including END, are acknowledged. A final ACK confirms receipt through END.
The receiver reports the hash result separately.

The receiver stays open to resend its final ACK if END is retried. Press
Ctrl+C and restart the receiver before each new transfer.

| Setting | Meaning |
| --- | --- |
| Sender positional `file` | Input file path. |
| `--host`, `--port` | Sender destination and receiver bind address, default `127.0.0.1:9000`. For two machines, bind the receiver to its local interface address and point the sender there. |
| Sender `--window` | Maximum outstanding distinct packets, including END when in flight. Default 8, range 1–4096. |
| Sender `--loss` | Probability of dropping each transmission attempt. Default 0.1. |
| Sender `--reorder` | Probability of holding an eligible attempt to send after the next surviving packet. Default 0.1. A hold expires after 100 ms. |
| Sender `--seed` | Optional random seed for repeating the simulator's choices. |
| Receiver `--output` | Destination path, default `received_file`. An existing file at that path is overwritten when the transfer completes. |

Both probabilities accept values between 0 and 1. For a clean baseline,
set both to 0. With 100% loss, use Ctrl+C to stop the sender.

## Implementation and tests

| Part | Contributor | Code |
| --- | --- | --- |
| 1 | Ghadi Zouki | `protocol.py`, `packetizer.py`, `channel.py` |
| 2 | Ali Hadi Zbeeb | `window.py`, `test_window.py`, `test_window_properties.py` |
| 3 | Yara El Rohayem | `receiver.py`, `test_receiver.py` |
| Integration | OpenAI-assisted wiring and review | `sender.py`, receiver CLI and cleanup, receiver test adjustments, and documentation |

[CONTRACT.md](CONTRACT.md) describes the wire format and how the three parts
interact. The source files and tests document the team's AI assistance,
including MiniMax M3 on Part 2 and Claude on Part 3.

Run the window unit and simulation tests with `python3 -m unittest discover -v`.
To test the receiver independently, start a fresh receiver on the default port
and run `python3 test_receiver.py`. The script sends early END and duplicate END,
and checks that ACKs are `1, 1, 1, 4, 4, 6, 6`.
