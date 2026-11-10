# EECE 350 — Reliable UDP File Transfer

Person 2's sender window is implemented against [CONTRACT.md](CONTRACT.md).
The repository currently contains this component and an in-memory demonstration;
the production packetizer, unreliable UDP channel, receiver, and sender CLI still
need to be integrated by the team. The contract is a proposed baseline, not proof
of teammate approval.

## Run Person 2's component demo

Python 3, standard library only; no installation or pip dependencies.
From the repository root (use `python` instead of `python3` on Windows if needed):

```bash
python3 -m unittest discover -v
python3 demo_person2.py fast
python3 demo_person2.py timeout
python3 demo_person2.py two-gaps
python3 demo_person2.py end-loss
python3 demo_person2.py final-ack-loss
python3 demo_person2.py reorder
python3 demo_person2.py empty
python3 demo_person2.py clean
```

These demonstrations use simulated time and an in-memory reference channel and
receiver. They exercise the real `SenderWindow`, with contract-shaped datagrams,
buffering, cumulative ACKs, and SHA-256 comparison in the reference receiver.
They do not send real UDP packets or write a received file to disk. The script
prints packet/window events, counters, and the reference receiver's hash result.

| File | Purpose |
| --- | --- |
| `window.py` | Person 2 controller: window, ACKs, RTT estimator, retries, stats |
| `demo_person2.py` | Readable scripted loss/reordering demonstrations |
| `tests/test_window.py` | Unit tests and complete in-memory transfer scenarios |
| `tests/harness.py` | Test-only packet preparation, channel, reference receiver |
| `docs/PERSON2_GUIDE.md` | Detailed explanation, integration example, oral practice |
| `AI_USAGE.md` | Provenance and assignment disclosure |

## Integration

Person 1 supplies `packets` (immutable encoded datagrams, END last) and
`channel.send(datagram, now)`. The sender loop validates ACK peer/type/format and
calls `on_ack(ack_next, time.monotonic())`. See the
[integration example](docs/PERSON2_GUIDE.md#10-how-your-teammates-connect-to-your-code).

The callback API and behavior follow the contract. Until Person 1 implements
`protocol.py`, `window.py` mirrors `MAX_WINDOW = 4096` locally to remain runnable
alone. Replace that definition with `from protocol import MAX_WINDOW` during
integration; do not change its value independently. Packet bytes remain opaque
to the controller; it never encodes, decodes, or sends a datagram through a socket.

Live sender/receiver CLI commands will be documented when those programs exist.
Whole-project acceptance still requires two actual UDP endpoints, disk output
verification, malformed/wrong-peer datagram handling, channel accounting, and
the assignment's combined display/summary. No claim of those checks passing is
made by the in-memory tests.

## Contribution ownership

- **Ali Hadi Zbeeb — Person 2:** assigned window/retransmission component.
  This initial implementation, tests, demo, and explanation were generated with
  OpenAI Codex assistance. Student review/adaptation has not yet been recorded.
- **Person 1:** packet format, packetizer, unreliable channel, sender CLI.
- **Person 3:** receiver, buffering, file reconstruction and verification.
- Yara El Rohayem / Ghadi Zouki's assignment to Persons 1 and 3 remains unconfirmed
  in the current contract.

The assignment requires the bulk of code to be student-developed and all AI/open
source use to be documented. Keep [AI_USAGE.md](AI_USAGE.md) accurate as the team
reviews and develops its submission.
