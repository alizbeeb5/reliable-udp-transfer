# Reliable UDP Transfer: Team Integration Contract

Version: 1.0 proposed baseline | Date: 2026-11-08 | EECE 350, Group 9

This document specifies what the three independently developed parts must exchange.
It is a proposal prepared for Ali's team, not a record of teammate approval.
The group should review it before implementing incompatible interfaces. Changes to
wire values or public interfaces must update this document and all affected parts
in the same coordinated change. Internal algorithms may differ if these guarantees hold.

## 1. Ownership and boundaries

| Part | Owner | Implements | Receives / provides |
| --- | --- | --- | --- |
| 1: packets, file preparation, unreliable channel | Yara El Rohayem or Ghadi Zouki; assignment pending | `protocol.py`, `packetizer.py`, `channel.py`, sender CLI wiring | Provides encoded packets and a `channel.send` callback to part 2 |
| 2: sending window, timers, retransmission | **Ali Hadi Zbeeb** | `window.py` | Takes encoded packets; receives validated ACK numbers; requests every transmission through the channel |
| 3: receiver, buffering, ACKs, file verification | Other teammate; assignment pending | `receiver.py` | Parses packets using `protocol.py`; sends cumulative ACKs back to the sender's UDP source address |

All three agree on `protocol.py`. Part 1 owns its implementation so there is one
definition of the wire format. Each author identifies their contribution in a
comment at the top of their source files. Shared edits identify their actual authors.
This contract was drafted with OpenAI assistance; it contains interface declarations
and examples, not a completed implementation. Document any AI-generated code used,
as required by the assignment. No implementation authorship is assigned by this document.

## 2. Scope and fixed conventions

- Python 3, standard library only; binary file input/output.
- One file, one sender, one receiver per run. Start the receiver first.
- UDP/IPv4; receiver default `127.0.0.1:9000`, configurable for two machines.
- Sender uses one socket bound to an ephemeral port for both outgoing packets and
  incoming ACKs. The channel uses that same socket. ACKs return to the observed
  sender address, not a second hard-coded ACK port.
- Sequence numbers count **packets, not bytes**, starting at zero. No wraparound.
- `CHUNK_SIZE = 1024`, `MAX_WINDOW = 4096`, default sender `window_size = 8`.
  Both programs import these constants. Require `1 <= window_size <= MAX_WINDOW`.
- Suggested demo defaults: `loss = 0.1`, `reorder = 0.1`, optional random seed.
  Accept probabilities in `[0, 1]`; 100% loss cannot complete without cancellation.
- No START packet, filename exchange, congestion control, SACK, or connection handshake.
  Receiver output path is a local command-line setting. Only DATA and END consume
  sequence numbers. ACKs do not.
- There is no transfer ID in version 1. Use a fresh sender socket and receiver state
  for each run; concurrent transfers or reuse with stale datagrams are out of scope.
  Receiver pins its peer to the first structurally valid DATA/END, which may arrive
  out of order. It ignores other peers. Sender accepts ACKs only from its configured peer.

## 3. Exact UDP datagram format

One UDP datagram contains exactly one header and its payload. Use
`HEADER = struct.Struct("!BIH")`; `HEADER.size == 7`.
The `!` means network byte order (big-endian), standard field sizes, no padding.

| Offset | Field / Python name | Struct code | Bytes | Meaning |
| --- | --- | --- | --- | --- |
| 0 | `packet_type` | `B` | 1 | `DATA = 1`, `ACK = 2`, `END = 3` |
| 1 | `seq` | `I` | 4 | DATA/END packet index, or ACK's next expected index |
| 5 | `payload_len` | `H` | 2 | Payload length in bytes, excluding header |
| 7 | `payload` | raw bytes | `payload_len` | File bytes, empty bytes, or raw SHA-256 digest |

| Type | `seq` | Payload | Total datagram size |
| --- | --- | --- | --- |
| DATA | `0 .. M-1` | 1 to 1024 file bytes | 8 to 1031 bytes |
| END | `M` | Exactly 32 bytes: `hashlib.sha256(file_bytes).digest()` | 39 bytes |
| ACK | `k`, next expected | Empty `b""`; length zero | 7 bytes |

`M` is the number of DATA packets: `(file_size + 1023) // 1024`.
All DATA payloads except possibly the last are 1024 bytes. END always exists,
including for an empty file (`M = 0`). Total reliable packets = `M + 1`.
END consumes a window slot. Final acknowledgement is **ACK `M + 1`**.
Require `M <= 2**32 - 2`, so the final ACK fits the unsigned 32-bit field.
The hash is the digest of original file bytes only; never include headers or END.
Use `.digest()`, not the 64-character `.hexdigest()`, on the wire.

Encoding is `HEADER.pack(packet_type, seq, len(payload)) + payload`.
Decoding rejects unknown types, short headers, trailing bytes, length mismatches,
zero-length/oversized DATA, nonempty ACK, and END payloads other than 32 bytes.
DATA/END sequence `2**32 - 1` is invalid because it cannot be acknowledged.
Malformed datagrams are logged and ignored without an ACK or state change.
Use `recvfrom(65535)` to avoid accidentally accepting a truncated oversized datagram.

### Byte-level examples (hex)

| Meaning | Entire UDP payload in hex |
| --- | --- |
| DATA 0 containing ASCII `abc` | `01 00 00 00 00 00 03 61 62 63` |
| ACK 1 | `02 00 00 00 01 00 00` |
| ACK 0, still missing packet 0 | `02 00 00 00 00 00 00` |
| END 1 after `abc` | `03 00 00 00 01 00 20 ba 78 16 bf 8f 01 cf ea 41 41 40 de 5d ae 22 23 b0 03 61 a3 96 17 7a 9c b4 10 ff 61 f2 00 15 ad` |

## 4. Cumulative ACK agreement

**ACK `k` means every packet with sequence `< k` has arrived contiguously,
and packet `k` is the next one needed. It never means "I received packet k".**

Receiver variable: `expected`, initially zero.

| Incoming valid DATA/END | Receiver behavior |
| --- | --- |
| `seq == expected` | Accept once, advance `expected`, then drain consecutive buffered packets; ACK the resulting `expected` |
| `expected < seq < expected + MAX_WINDOW` | Buffer first copy by sequence; do not write it yet; send ACK `expected` |
| `seq < expected` | Discard duplicate; send ACK `expected` again |
| Already buffered `seq` | Keep first copy, do not duplicate data; send ACK `expected` |
| `seq >= expected + MAX_WINDOW` | Discard outside receive window; send ACK `expected` |

Write DATA in sequence order only. A gap-filling arrival produces one ACK after
draining the consecutive buffer, not one ACK for every buffered packet drained.
Do not wait for packet zero before buffering other valid packets.

Example: receive DATA `0, 2, 3, 1` -> emit ACKs `1, 1, 1, 4`.
The last ACK acknowledges all four packets. No selective ACK information is transmitted.

## 5. END, verification, and lost final ACK

END is handled by the same ordering/buffering rules as DATA. If it arrives before
a gap is filled, buffer it and keep ACKing the missing sequence. Receiving END
alone does not complete the file. Once END `M` becomes contiguous:

1. All DATA `0 .. M-1` must have been written exactly once. Finish/flush the output,
   close it, and compute SHA-256 from the bytes actually written to disk.
2. Compare with the stored END digest; emit a receiver summary with MATCH or MISMATCH.
   Keep a mismatching output for inspection and clearly mark the run unsuccessful.
3. Advance `expected` to `M + 1` and send ACK `M + 1` after the verification attempt.
   A digest mismatch still gets this ACK: ACK confirms contiguous transport receipt,
   **not** a successful hash comparison. A local file I/O error is fatal: log it,
   send no completion ACK, and require the sender to be cancelled/restarted.
4. Enter completed state and keep the receiver socket open until the operator stops
   it. Re-ACK valid duplicate DATA/END from the pinned peer with ACK `M + 1`, without
   rewriting, reopening, or rehashing the file. Reject packets beyond END; do not
   start another transfer implicitly. Buffered entries beyond END are discarded.

Staying available lets the sender recover a lost final ACK by retransmitting END.
There is no finite retry limit or auto-exit timeout in version 1. Sender cancellation
is reported as incomplete, never successful. After final ACK, sender displays
`TRANSPORT COMPLETE; hash result shown at receiver`. It must not claim MATCH from
an ACK with no status field. Show both endpoints in the demo/final combined summary.

## 6. Ali's sending-window contract

Let `P = len(packets) = M + 1`. Maintain:

| Variable | Initial value | Meaning / invariant |
| --- | --- | --- |
| `base` | 0 | Oldest unacknowledged sequence |
| `next_seq` | 0 | Next never-submitted packet |
| `window_size` | CLI value | Maximum outstanding unique packets |
| `dup_ack_count` | 0 | ACKs equal to current `base` while data is outstanding |
| `fast_retransmitted_base` | `None` | Prevent repeated fast retransmits for the same base |
| `timer_deadline` | `None` | One timer, for the oldest outstanding packet |

Always `0 <= base <= next_seq <= P` and `next_seq - base <= window_size`.
New sends are allowed while `next_seq < min(base + window_size, P)`.
Record the attempt and advance `next_seq` before handing bytes to the channel;
a simulated drop or hold still counts as a transmission attempt.

For each submitted packet retain `attempts`, `first_sent_at`, `last_sent_at`, and
`acked`. `attempts > 1` means it has been retransmitted. Its current display state
is ACKNOWLEDGED if `acked`, otherwise RETRANSMITTED if `attempts > 1`, otherwise SENT.
Never-submitted slots may display UNSENT. Preserve retransmission history after ACK.

For a validated ACK `a` from the correct peer:

- `base < a <= next_seq`: mark `[old_base, a)` acknowledged, set `base = a`,
  reset duplicate count and fast-retransmit guard, and update the timer. The next
  fill operation may send more packets.
- `a == base` and `base < next_seq`: increment duplicate count. The third such
  ACK triggers one fast retransmission of packet `base` through the channel.
  Set the guard; further equal ACKs do not trigger another fast retransmission
  until `base` advances. Timeout retransmission remains possible.
- `a < base`: stale ACK; log/ignore; do not increment or reset duplicate count.
- `a > next_seq`: ACK for unsent data; log/ignore without changing any state.
- No outstanding packets: equal ACKs do not count as duplicates.

An ACK that advances `base` is not itself a duplicate. At startup `base = 0` is
the reference: three received ACK 0 packets trigger fast retransmit even without
a preceding ACK 0. Ignore malformed and wrong-peer ACKs entirely.
Do not implement Go-Back-N: on timeout or fast retransmit, resend **only `base`**.
Completion is `base == P`, not "all packets were sent" or "END was sent".

### Timer and RTT rules

Use `time.monotonic()` and seconds throughout. Inspired by
[RFC 6298, sections 2, 3, and 5](https://www.rfc-editor.org/rfc/rfc6298.html);
this is a simplified educational protocol, not a full TCP implementation.

- Initial RTO = 1 second; clamp to `[1, 60]`; `G = 0.01` second.
- First eligible RTT `R`: `SRTT = R`, `RTTVAR = R / 2`.
- Later: `RTTVAR = 0.75 * RTTVAR + 0.25 * abs(old_SRTT - R)`;
  then `SRTT = 0.875 * old_SRTT + 0.125 * R`.
- After a sample: `RTO = clamp(SRTT + max(G, 4 * RTTVAR), 1, 60)`.
- Conservatively sample only an ACK advancing exactly one packet whose
  `attempts == 1`: `R = ack_time - first_sent_at`. Skip cumulative jumps and
  retransmitted packets to avoid ambiguous measurements.
- First outstanding send starts `deadline = now + RTO`. New sends and duplicate
  ACKs do not restart it. Advancing ACKs restart it if anything remains outstanding;
  otherwise stop it.
- Timeout: count one timeout, retransmit `base`, double RTO (cap 60), and restart.
  Do not clear backoff without a new eligible sample.
- Fast retransmit: restart deadline with current RTO; do not double RTO.

Timestamps start at channel submission, even if the simulator delays the packet.
Process already available ACKs before checking expiry. A fast retransmit in that
iteration resets the timer, preventing an immediate second timeout retransmission.

## 7. Unreliable channel contract (part 1)

Part 2 never calls `socket.sendto`. Every original or retransmitted DATA/END goes
through `channel.send(datagram, now)`, which returns `None`, not delivery success.
The window must learn delivery only from ACKs. Simulator events are for display,
not shortcuts for sliding the window or immediate retransmission.

Concrete policy, with independent random draws where called for:

1. At each submission, first service an expired held packet as described below.
   Draw for loss. If the draw is below `loss`, drop this submission and return.
2. If a packet is held and the new submission survives loss, send the new packet
   immediately, then the held packet. Do not perform a reorder draw for the new
   packet in this case. Clear the hold and log the actual reversed pair.
3. Otherwise draw for reorder. Below `reorder`, hold this packet; otherwise send
   immediately. The channel holds at most one datagram at a time.
4. Set `hold_deadline = now + 0.1` seconds when holding. `tick(now)` releases an
   expired hold even if no subsequent packet arrives. A forced idle release logs
   `FLUSH`, not `REORDERED ... after ...`. This prevents window-size-1 and final-END
   deadlocks. Dropped submissions do not release an unexpired hold.

Releasing a held packet does not redraw loss/reorder: it is the same attempt.
Resubmitting it later for retransmission is a new attempt and gets fresh decisions.
ACKs bypass the artificial channel; normal UDP loss/reordering is still possible.
Socket errors propagate as fatal local errors; do not label them simulated drops.
After sender completion, service any remaining hold until empty before closing
the socket, without creating additional transmission attempts.

## 8. Public Python interfaces and runtime wiring

The declarations below specify names, arguments, and results; they are not runnable
implementations. Use immutable bytes for packets. File preparation happens once,
before timing the transfer. Version 1 loads the prepared packets into memory.
Calculate the digest and file size from the same bytes used to build these packets;
do not hash a separate reread that could differ if the input file changes.

```python
# protocol.py -- part 1, shared by everyone
DATA = 1
ACK = 2
END = 3
CHUNK_SIZE = 1024
MAX_WINDOW = 4096
HEADER = struct.Struct("!BIH")

@dataclass(frozen=True)
class Packet:
    packet_type: int
    seq: int
    payload: bytes

def encode_packet(packet_type: int, seq: int, payload: bytes = b"") -> bytes: ...
def decode_packet(datagram: bytes) -> Packet: ...  # ValueError on invalid input

# packetizer.py -- part 1
@dataclass(frozen=True)
class PreparedTransfer:
    file_name: str
    file_size: int
    sha256_digest: bytes
    packets: tuple[bytes, ...]  # index == seq; final item is END

def prepare_transfer(path: str) -> PreparedTransfer: ...

# channel.py -- part 1
class UnreliableChannel:
    def __init__(self, sock, peer, *, loss, reorder, seed, emit): ...
    def send(self, datagram: bytes, now: float) -> None: ...
    def tick(self, now: float) -> None: ...
    def has_pending(self) -> bool: ...
    def stats(self) -> dict: ...

# window.py -- Ali / part 2
class SenderWindow:
    def __init__(self, packets, *, window_size, send_packet, emit): ...
    def fill_window(self, now: float) -> None: ...
    def on_ack(self, ack_next: int, now: float) -> None: ...
    def on_tick(self, now: float) -> None: ...
    def done(self) -> bool: ...
    def snapshot(self) -> dict: ...
    def stats(self) -> dict: ...
```

- `send_packet` is the bound `channel.send` method, signature `(bytes, float) -> None`.
  It must not synchronously call `on_ack`. Constructor performs no sends. Invalid
  configuration raises `ValueError` before transfer starts.
- `fill_window` submits never-sent packets until full; calling it twice cannot
  resend them. `on_ack` handles ACK state and any fast retransmit but does not fill
  the window. `on_tick` handles an expired timer once per call.
- `snapshot()` returns `base`, `next_seq`, `window_size`, `rto`, `timer_deadline`,
  `dup_ack_count`, and `packets` (list of records with `seq`, `state`, `attempts`).
  Include current window slots, plus acknowledge transitions in the event log.
  Snapshot/stats are copies; displays must not mutate controller state.
- `emit(event: dict) -> None` is a shared logging callback. Every event has `time`
  (monotonic float), `side` (`sender`, `channel`, or `receiver`), and `event` (string).
  Include applicable `seq`, `ack_next`, `reason`, `base`, `next_seq`, `attempt`.
  Timer events include `rto`; window events include packet states. The renderer
  converts these fields into readable terminal lines. It must not alter protocol state.

Sender CLI/main (part 1, integrated with Ali) owns the socket and ACK read loop:

1. Prepare packets, construct channel/window, print settings, and call `fill_window`.
2. Wait for datagrams with a maximum blocking interval of 0.01 seconds. Validate
   peer, decode, require ACK type, and pass `packet.seq` to `window.on_ack`.
   Service a bounded batch of ready ACKs so timers cannot starve.
3. With fresh monotonic timestamps, call `channel.tick`, `window.on_tick`, then
   `window.fill_window`, and render changes. Repeat until `window.done()`.
4. Record completion time at final ACK, drain any channel hold, and print summary.

Only this loop mutates sender/channel state; no timer thread is necessary. A GUI,
if added, consumes event/snapshot copies. Part 3 owns its separate receiver loop,
calling `decode_packet` and `encode_packet(ACK, expected)` and sending to the peer.
Both CLIs handle cancellation and local I/O errors without declaring success.

## 9. Statistics and display definitions

| Field / owner | Exact meaning |
| --- | --- |
| `data_packets` / setup | `M`, excludes END |
| `total_packets` / setup | `M + 1`, includes END |
| `transmissions` / window | Every original or retransmission submission to channel, including dropped/held attempts |
| `retransmissions` / window | Submissions for a sequence already attempted; subset of transmissions |
| `timeouts` / window | Timer expirations that trigger a retransmission |
| `fast_retransmits` / window | Retransmissions triggered by three duplicate ACKs |
| `acked_packets` / window | `base`, including END when complete |
| `transfer_time_s` / window | First submission to final ACK; excludes packet preparation and channel cleanup; elapsed time so far while active |
| `submitted` / channel | Calls to `send`; equals window transmissions |
| `dropped` / channel | Submission attempts discarded by simulated loss |
| `held` / channel | Attempts selected for holding |
| `reordered` / channel | Held attempts actually released after a later submitted attempt |
| `flushed` / channel | Held attempts released at their hold deadline without a later surviving attempt |
| `udp_sent` / channel | Actual successful `sendto` calls, including releases |
| `pending` / channel | 0 or 1 currently held attempt |
| `bytes_written`, `verification` / receiver | Unique ordered file bytes; `PENDING`, `MATCH`, `MISMATCH`, or `IO_ERROR` |

On non-error runs: `submitted = dropped + udp_sent + pending`;
`held = reordered + flushed + pending`;
`retransmissions = timeouts + fast_retransmits`.
On successful transport completion: `transmissions = total_packets + retransmissions`.
Receiver ACK sends are tracked separately, not added to sender packet counts.

Show file name/size, DATA and total packet counts, N, probabilities, seed, and
addresses at sender startup. Receiver shows its output path/address and reports
its own final byte count. It cannot know the original filename/size at startup
because version 1 does not send metadata. Log SEND, DROP, HOLD, REORDERED, FLUSH,
ACK, DUP_ACK, TIMEOUT, RETRANSMIT, WINDOW, receive/buffer/discard events and verification.

## 10. Integration acceptance checklist

Use scripted channel decisions/fake monotonic times for exact behavior checks;
use a fixed seed only for repeatable demo runs. Each owner tests their component
against the same cases before integration. This is a checklist, not a claim tests pass.

| Case | Required observation |
| --- | --- |
| Golden bytes in section 3 | Both endpoints encode/decode the exact same bytes |
| Empty file | END 0, ACK 1, SHA-256 of empty bytes matches |
| Sizes 1, 1024, 1025 bytes and binary input | Correct chunk count/length; no padding or text conversion |
| Loss/reorder zero | No retransmissions; output bytes equal input; both report completion correctly |
| N = 1, reorder = 1 | Held packet flushes; transfer progresses including END |
| Receive `0, 2, 3, 1` | ACK `1, 1, 1, 4`; file writes `0, 1, 2, 3` exactly once |
| Drop DATA 0; N >= 4; deliver 1, 2, 3 before timeout | ACK `0, 0, 0`; one fast retransmit of 0; repeated ACK 0 does not loop |
| Two gaps in one window | First gap repaired; cumulative ACK exposes next base; timer repairs second gap |
| Timeout | Only base retransmitted; RTO backs off; duplicate/stale ACKs cannot postpone timeout |
| RTT sample | 0.2s then 0.3s yields SRTT 0.2125, RTTVAR 0.1, clamped RTO 1s; retransmitted packet yields no sample |
| ACK skips buffered run | All sequences below ACK become acknowledged; window refills without exceeding N |
| Stale, unsent-range, malformed, wrong-peer ACK | No invalid window movement or retransmission trigger |
| END arrives before missing DATA | END buffered; completion ACK withheld until gap repaired and verification attempted |
| END lost | END retried through channel until ACK `M + 1` |
| Final ACK deliberately dropped | Receiver remains available; duplicate END gets same final ACK without duplicate file writes |
| Digest deliberately wrong | Receiver reports MISMATCH; sender never infers MATCH from completion ACK |
| Malformed datagrams / duplicates | No crash, invalid ACK, or duplicate output bytes |
| Receiver I/O failure / 100% loss | Explicit error or cancellable incomplete run, never false success |
| Counters after normal cleanup | Section 9 identities hold and `pending == 0` |

## 11. Team review and handoff

- Confirm which of Yara/Ghadi owns part 1 versus part 3; only Ali's part 2 is confirmed.
- Agree on this document's values before independent implementation. Any change
  to ACK semantics, packet types, hash encoding, or callback signatures requires
  all affected owners to coordinate, even if the change looks small.
- Part 1 hands Ali the packet tuple, channel callback, and ACK dispatch loop.
  Ali can start against a fake callback and scripted ACKs before sockets exist.
  Part 3 can start against the golden datagrams before the sender exists.
- Add README run commands once the CLI exists, actual contribution records, and
  the required instructor collaborator invitations. This contract does not claim
  those invitations or any other deliverables have been completed.

Assignment source: supplied `EECE 350 Project-4.pdf`, project overview and member
tasks (pages 1-3), plus display/deliverables (page 3). The assignment requires the
header, cumulative ACK behavior, base timeout, fast retransmit, END hash and
retransmission, and sender channel boundary. Numeric wire values, module names,
defaults, simulator edge-case policy, and runtime interfaces here are proposed
team choices, not additional instructor requirements.
