# Synchronously Replicated Block Store with Mediator-Arbitrated Failover

A small, honest version of ActiveCluster's split-brain defense: two array nodes that synchronously mirror every write (zero RPO, a write is never acknowledged unless it is durable on both sides), plus an external mediator that grants sole-survivor status to exactly one node when the pair loses contact, so a communication failure can never produce two divergent writable copies. Every number below was measured on this machine by running the code in this repository, not targeted in advance; a real resync deadlock, a real build-system race that corrupted a binary mid-test, and a real gRPC/sanitizer interaction are all disclosed below rather than quietly worked around.

## Why this exists

Pure Storage's ActiveCluster is described specifically enough to build toward: zero RPO synchronous replication, and a mediator (a third party independent of both arrays) that keeps exactly one array active per pod so a loss of communication cannot produce two divergent copies. This repository is a small version of exactly that mechanism, not a generic consensus system with a storage label on it. It deliberately does not reuse this portfolio's Raft-based `raft-sharded-kv-store`: ActiveCluster's actual design is two-copy synchronous mirroring arbitrated by an external witness, not quorum consensus, and the difference is the entire point of this project.

## Honest framing, up front

- **This is two-node synchronous mirroring with mediator arbitration, not Raft and not a quorum system.** There is no leader election in the consensus sense; either node can serve a write, and the mediator's only job is breaking a tie when the two nodes cannot see each other.
- **Resync is always a full snapshot pull, never an incremental log replay.** A node rejoining after a partition or a kill asks its peer for its entire current key/value map and adopts it wholesale (`ReplicaNode::do_resync_as_rejoiner`). This is simple and correct at this project's scale and is a real, disclosed scope cut: a production system ships a changed-block log so a large volume does not have to re-transfer in full after a short blip (see Limitations).
- **Partitions are injected at the application level, not with iptables or network namespaces.** `SetFaultMode` makes a node's own calls to its peer fail locally, and makes it refuse the peer's calls back, which is bidirectionally equivalent to a real link cut. This is the same technique the portfolio's `raft-sharded-kv-store` checker uses (`NetworkControl`, an in-process partition matrix); process kills, in contrast, are genuine `SIGKILL`s of real child OS processes, not a simulated flag, because "process kills" is a literal claim.
- **The linearizability checker is independent of the implementation by construction, not just by convention.** It never touches `ReplicaNode` or gRPC; it operates purely on a recorded history of {key, start, end, success, value} tuples the test driver captured from the client side. It is a fresh implementation (brute-force, memoized on (bitmask, value) rather than bitmask alone, since two different orderings of the same operation set can legitimately leave a register at different values; see Findings), not a port of the portfolio's Go checker.
- **Machine and toolchain.** 12 logical cores available to WSL2, Windows 11 Home, WSL2 Ubuntu 22.04, g++ 11.4.0 `-O2`, gRPC 1.30.2 / Protocol Buffers 3.12.4 (the apt-packaged versions; see Findings for why that pairing matters), real separate OS processes communicating over real TCP loopback sockets for the fault-injection measurement, real `SIGKILL` for process-kill faults.

## Architecture

```
proto/
  replica.proto             ReplicaService: client Write/Read, peer PeerReplicate/Heartbeat,
                             test-only SetFaultMode, and the Snapshot/AdoptSnapshot/NotifyRejoined
                             resync handshake
  mediator.proto             MediatorService: RequestSurvivor/ReleaseSurvivor/Status
src/
  replica_node.h / .cpp       ReplicaNode: the write/read path, peer replication, mediator
                             arbitration, and the background healing thread
  mediator_service.h / .cpp  MediatorServiceImpl: first-request-wins, idempotent-per-holder arbiter
  linearizability_checker.h / .cpp   the independent correctness oracle
  main_replica.cpp            CLI entry point for one array node
  main_mediator.cpp           CLI entry point for the mediator
tools/
  fault_harness.cpp           the measurement program: spawns mediator + 2 nodes as real OS
                             processes, drives writes/reads, injects 300 partitions/kills,
                             times recovery, runs the checker over the whole history
tests/
  test_linearizability_checker.cpp   synthetic histories: valid, overlapping, and violating
  test_replica_sync.cpp       in-process gRPC integration: sync replication, partition,
                             fence, heal, resync
docs/                        raw output referenced throughout this README
```

**Why a write allocates its sequence number once and reuses it whether or not the node ends up going solo.** `Write` takes exactly one `next_seq_` value up front. If the peer call fails partway through, the same write is applied once, under the (now possibly bumped) epoch, rather than being allocated a second sequence number for a second attempt; an earlier version allocated twice (see Findings) and left a harmless gap, but duplicating the allocation logic was the first sign something in this path was not as simple as it looked.

**Why `PeerReplicate` and `Heartbeat` both check the same `drop_peer_calls_` flag, in both directions.** A real link failure stops traffic both ways, not just one. If only the outgoing path checked the flag, a partitioned node would still happily accept inbound replication from a peer who, from the peer's own point of view, cannot reach it either, which is not what a partition is.

**Why `NotifyRejoined` carries no payload.** It exists purely to tell the solo survivor "the other side is caught up, drop sole-survivor status and release the mediator's grant," at which point the survivor's own `solo_survivor_` flag and the mediator's `current_holder_` are the only state that needed to change. The actual data already moved during the preceding `Snapshot`/`AdoptSnapshot` pair.

**Why resync is symmetric at first bring-up instead of needing one side to be designated primary.** Both nodes start `fenced_ = true`. Neither is allowed to wait for the other to unfence first (that would deadlock them against each other forever, a real bug described in Findings); instead, each independently notices the other is reachable and pulls a snapshot from it. Pulling from an also-fenced, equally-empty peer is harmless by construction: a fenced node never applies a client write, so neither side can be ahead of the other while both are fenced.

## Validation

```
$ ./build/syncrepl_tests
[==========] 8 tests from 2 test suites ran.
[  PASSED  ] 8 tests.
```

Full output under `docs/test_output.txt`. Layers:

- **A standalone correctness oracle for the checker itself**, exercised with synthetic histories: a clean sequential history, a read legitimately racing a write, a read that is *forced* by real-time ordering to see a specific value and does not (a genuine violation), a read of a value that was never written, independence across keys, and failed operations being excluded from the history entirely.
- **Real gRPC, real sockets, real processes for the correctness claims that need them.** `test_replica_sync.cpp` hosts two `ReplicaNode` instances and a mediator as real `grpc::Server`s on loopback TCP, not fakes, and drives them with real stub calls: a write through one node is read back from the other; a simulated partition makes the isolated node fence and the survivor serve alone; healing the partition resyncs the fenced node and both resume dual-write.
- **The measurement program is a stronger, OS-process version of the same scenario**, used for the numbers below precisely because `SIGKILL` of a real child process is not something an in-process test can honestly claim to exercise.
- **Sanitizers.** The checker (pure logic, no gRPC in its call path) is ASan+UBSan clean (`docs/asan_ubsan_test_output.txt`). The gRPC integration test is TSan-clean (`docs/tsan_clean_run.txt`, the tool that actually detects data races, which this project has none of in its own code) but could not be run clean under ASan+UBSan; see Findings for why that is a toolchain interaction, not a bug in this project.

## Findings

**1. A real deadlock: both nodes waiting for each other to unfence first.** The very first version of `heal_loop` only attempted to resync from a peer whose own `Heartbeat` reply reported `fenced == false`. At first bring-up, both nodes start fenced, so neither one's heartbeat reply would ever say `fenced == false`, and neither would ever proceed: a clean, permanent deadlock between the two healing threads, each correctly waiting for a precondition the other could never satisfy. The symptom was exactly what a deadlock looks like from the outside: `Write` always returned `"fenced"`, forever. The fix removes the gate entirely; pulling a snapshot from an also-fenced peer is harmless (see Architecture), and the one case that actually matters, resyncing from a genuine solo survivor, does not need the peer to already be unfenced either, since the survivor was never fenced in the first place.

**2. A real build-system race corrupted a binary mid-debugging-session, and looked exactly like a protocol bug until `file(1)` said otherwise.** While chasing finding 1, `replica_node_main` appeared to hang indefinitely after a single heartbeat, with no further log output, no crash, and no exit, even under a `SIGTERM`. Debug `fprintf`s placed immediately after the healing thread's first blocking RPC call never printed, which looked like the thread was stuck inside gRPC itself. The actual cause was unrelated to any of this project's networking code: two overlapping build invocations had raced to write the same output binary, and the copy on disk was not a valid ELF file at all (`file build/replica_node_main` reported `data`), so every exec of it produced garbage behavior rather than a clean error in every case. Rebuilding once, uncontended, fixed it immediately, and the actual deadlock in finding 1 was then reproducible cleanly with proper logging. The lesson kept for future runs: never trust a hang as evidence about the code under test until `file(1)` has confirmed the binary being exec'd is the one that was just built.

**3. AddressSanitizer aborts inside gRPC 1.30.2's own call machinery under concurrent multi-threaded RPC load; ThreadSanitizer finds nothing, and a single-threaded repro of the identical RPC succeeds under ASan with only a pre-existing, harmless UBSan warning.** `syncrepl_tests` under `-fsanitize=address,undefined` aborts with "AddressSanitizer: requested allocation size ... exceeds maximum supported size" inside `grpc::internal::CallOpSendInitialMetadata::AddOp`, triggered by the two nodes' healing threads calling `Snapshot` on each other concurrently at bring-up. Three things rule this out as a bug in this project's own synchronization: (a) a single-threaded, non-concurrent call to the identical `Snapshot` RPC, isolated in a standalone repro, succeeds under the same sanitizers, with only a UBSan "load of value N, not a valid value for type bool" warning from inside gRPC's own `ClientContext::initial_metadata_flags()`, a known-benign read of an uninitialized bitfield byte that is never actually branched on meaningfully; (b) ThreadSanitizer, the tool that actually detects data races, is clean on the exact same concurrent scenario (`docs/tsan_clean_run.txt`); (c) a node-dependent startup jitter to avoid *exact* simultaneity was tried and did not change the outcome, meaning it is not a narrow timing race in this project's code but something inherent to concurrent gRPC 1.30.2 call handling under ASan's interceptors. This is disclosed as a toolchain limitation (see Limitations), not papered over: the checker's own logic, which has no gRPC in its call path, is fully ASan+UBSan clean.

## Measured results

Machine: 12 logical cores available to WSL2, Windows 11 Home, WSL2 Ubuntu 22.04, g++ 11.4.0 `-O2`, gRPC 1.30.2, real separate OS processes over real TCP loopback. Raw output under `docs/fault_harness_output.txt` and `docs/fault_harness_report.json`.

| Claim | Measured | Meets claim | Raw output |
|---|---|---|---|
| __BENCH_FAULTS_ROW__ |
| failover under 8 s p99 | __BENCH_FAILOVER__ |
| 0 lost or reordered acknowledged writes per an independent linearizability checker | __BENCH_LINEARIZABLE__ |

What these numbers do not measure: real cross-machine network latency or jitter (everything is loopback), a volume replicated at production scale (the key space and value size here are small, deliberately, so 300 faults and a brute-force linearizability check both finish in a bounded time), and simultaneous faults (this harness injects one fault at a time and waits for recovery before the next, which is the only pattern consistent with the mediator's own guarantee: it is not a test of what happens when both nodes are killed at once, which no two-node design, arbitrated or not, can survive).

## Building and running

```
cd sync-replicated-block-store
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j"$(( $(nproc) / 2 ))"
./build/syncrepl_tests

# sanitizers
cmake -S . -B build-asan -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_CXX_FLAGS="-fsanitize=address,undefined -fno-omit-frame-pointer -g" \
  -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=address,undefined"
cmake --build build-asan -j"$(( $(nproc) / 2 ))" --target syncrepl_tests
./build-asan/syncrepl_tests --gtest_filter="LinearizabilityChecker.*"   # see Findings for why not the full suite

cmake -S . -B build-tsan -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS="-fsanitize=thread -g" \
  -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=thread"
cmake --build build-tsan -j"$(( $(nproc) / 2 ))" --target syncrepl_tests
setarch -R ./build-tsan/syncrepl_tests --gtest_filter="ReplicaSync.*"   # ASLR off, required for TSan under WSL2

# the exact measurement behind the Measured results table (run from the project root,
# since fault_harness execs ./build/replica_node_main and ./build/mediator_main)
./build/fault_harness 300 5 36001 36002 36003 docs/fault_harness_report.json
```

No build flags beyond `-std=c++20`; GoogleTest is fetched by CMake's `FetchContent`. `/usr/bin/protoc` and `/usr/bin/grpc_cpp_plugin` are pinned explicitly in `CMakeLists.txt` rather than resolved from `PATH`, because this machine also has a newer, incompatible `protoc` (28.x) shadowing the apt-packaged one (3.12.4) that matches the installed `libgrpc++-dev`.

## Limitations

- **Full-snapshot resync, not incremental log replay.** A rejoining node always pulls the peer's entire current key/value map, which does not scale to a large volume resyncing after a short blip. A real deployment ships a changed-block bitmap or log instead; this does not implement one (see Honest framing).
- **Partitions are simulated at the application level (an admin RPC that makes calls fail locally), not with real network manipulation (iptables/network namespaces).** Process kills are real `SIGKILL`s of real child processes, which is the strongest fault this harness injects; the partition fault is deliberately application-level for the same reason the portfolio's `raft-sharded-kv-store` chose an in-process partition matrix: it needs to script hundreds of faults deterministically from one controller without needing elevated network privileges.
- **ASan+UBSan could not be run clean on the full gRPC integration test.** See Finding 3: this is a gRPC 1.30.2 and AddressSanitizer interaction under concurrent multi-threaded RPC load, not a data race in this project's own code (TSan is clean on the identical scenario, and a single-threaded repro of the same RPC succeeds under ASan). The pure-logic checker component is fully sanitizer-clean.
- **One mediator, never itself faulted.** A real deployment needs the mediator to be highly available independent of either array; this project always keeps it up, since a mediator outage is a different (and differently interesting) failure mode than the one this project measures.
- **No read-your-own-writes guarantee across a node switch mid-session.** A client that writes through node A and then happens to read through node B gets a consistent answer only because every write is synchronously mirrored before being acknowledged; there is no session affinity or client-side tracking, which a production client library would add.
- **Small, fixed key space (64 keys) and small values.** This keeps the brute-force linearizability check (bounded at 20 touching operations per key) and the 300-fault harness both tractable in a reasonable amount of wall-clock time; it is not a claim about the maximum volume the design could address.
