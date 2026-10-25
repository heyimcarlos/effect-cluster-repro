# Effect Cluster reproductions

Six scripts, one per issue. Each runs real runner processes
(`NodeClusterSocket.layer` with `SqlMessageStorage` and `SqlRunnerStorage` on
PostgreSQL). Each script prints timestamped logs from the runners (shard
assignments, registration, shutdown) and from itself (pg_locks samples, message
rows). It ends with the expected result, the actual result, and `PASS` or
`FAIL`, and exits with 1 when the bug reproduces.

## Versions

| Build | What |
| --- | --- |
| release | `effect`, `@effect/sql-pg`, `@effect/platform-node` 4.0.6 from npm (the latest release) |
| main | Effect-TS/effect `main` at `837e906d9a476bff12fbd063d1635f748e1de7de` (2026-10-25), built and packed by `scripts/build-main.sh` |

Node 24 runs the `.ts` files directly. PostgreSQL 17 runs in Docker.

## Setup

```sh
docker compose up -d          # PostgreSQL on 127.0.0.1:55642
npm install
./run-all.sh                  # release; full logs in logs/stock/
scripts/build-main.sh         # clones Effect, builds main into .build/main (needs pnpm)
./run-all.sh main             # full logs in logs/main/
docker compose down -v
```

To run one script: `node src/issue3-stale-owner-writes.ts`. `REPRO_DATABASE_URL`
overrides the database. Each script drops and recreates the database named in
that URL (`repro` by default), so point it only at a scratch server. The
scripts use ports 34431 to 34462.

The scripts use short lock timings (`shardLockExpiration` 5 s, refresh 1 s,
`refreshAssignmentsInterval` 500 ms) and 2 to 24 shards. They are in
`baseConfig` in `src/lib.ts`.

## Results

| # | Issue | release 4.0.6 | main 837e906 |
| --- | --- | --- | --- |
| 1 | Replay decides a recorded race again after a handoff | FAIL: `Complete("text")`, ran `claim` | FAIL: same |
| 2 | Run interrupted at shutdown stays Suspended | FAIL: Suspended 20 s after B owns the shards, replied=2 unprocessed=0 | FAIL: same |
| 3 | Stale owner still writes after losing its shard | FAIL: A's reply stored, client gets `"A"` | FAIL: same |
| 4 | Frozen runner's advisory locks never expire | FAIL: B holds 6/12 shards after 30 s frozen | FAIL: same |
| 5 | Runners joining one at a time move shards more than once | FAIL: 18 moved, 7 moved 2 or 3 times | FAIL: same |
| 6 | Shard-move pause after SIGTERM | PASS: idle entity waits under 1 s; busy shard held for the full timeout (see below) | PASS: same |

## Issues

**1. A workflow replay decides a recorded race again after a shard handoff.**
`src/issue1-race-replay.ts`. A workflow runs `DurableDeferred.raceAll` over an
external deferred (`gate`) and a one-hour `DurableClock.sleep`. It then runs
activity `claim` if the gate won or `close` if the clock won. Runner A suspends
the run and stops. Runner B takes the shards, the gate is completed, and B's
replay pauses before the race. The script then stores the race result `idle`
in the same form the engine uses: a `deferred` request `raceAll/wait`, copied
from the engine's own `gate` row. A previous owner leaves this row when it has
persisted its clock result but stops before processing it. The script writes
the row directly because that timing is hard to hit on demand. On replay,
`deferredResult` reads only processed replies. B does not see the stored
result, races again, and the gate wins. The durable workflow takes a different
branch from the one already recorded, and its history holds both results.
Expected: `Complete("idle")`, activities `[close/1]`. Actual:
`Complete("text")`, activities `[claim/1]`, while the stored `raceAll/wait` row
says `idle`.

**2. A run interrupted at shutdown stays Suspended forever.**
`src/issue2-interrupted-run-parked.ts`. The workflow races its first activity
against an external deferred. On runner A the body waits until
`Sharding.isShutdown` is true and then starts the activity. A has received
SIGTERM while the run starts its first activity. The interrupted race reads as
parked, so the run's reply is saved as `Suspended`. No deferred or clock will
wake it. After A exits, storage holds `run` replied `Suspended` and
`activity/step/1` unprocessed. Runner B takes the shards and answers the
activity request with `Suspended` too. The run then has `replied=2
unprocessed=0` and never moves. Expected: the run completes on B
(`Complete("done")`). Actual: still `Suspended` 20 s after B owns the shards.
A rolling deploy can strand runs this way.

**3. No write fencing: a stale owner still writes after losing its shard.**
`src/issue3-stale-owner-writes.ts`. Runner A starts a persisted request whose
handler waits on a gate. A is frozen (SIGSTOP), and its advisory-lock backend
is ended with `pg_terminate_backend`, as a network partition would end it.
Runner B acquires every shard and starts the same request. A is resumed with
its gate open and finishes first. Shard ownership is checked only in memory,
so storage accepts A's reply. Two runners ran the request, and the kept reply
came from the runner that no longer owns the shard. Expected: A's reply is
rejected, and the stored reply and the client's result come from B. Actual:
once A finishes, the row holds `{"_tag":"Success","value":"A"}` before B's
handler returns. The client receives `"A"`, and B's later reply is dropped.

**4. A frozen runner's advisory-lock session never expires, so its shards
never move.** `src/issue4-frozen-runner-locks.ts`. Two runners split 12
shards. A is stopped with SIGSTOP for 30 s (`FREEZE_SECONDS`), which is six
lock expirations. About 5 s after the freeze, B drops A from the healthy
runners and assigns itself all 12 shards (B logs `New shard assignments` with
12 shards). A's advisory locks belong to A's Postgres session, which stays open
and idle, so B can never acquire them. The script samples `pg_locks` every
second, and every sample from the freeze to the end shows each session holding
6 locks. A paused VM, a long GC stop, or a host that loses its network while
its TCP session stays up blocks those shards until the session ends. Expected:
B holds all 12 locks about `shardLockExpiration` (5 s) after the freeze.
Actual: B holds 6/12 after 30 s.

**5. Runners joining one at a time move shards more than once.**
`src/issue5-join-moves.ts`. Runner A holds 24 shards, and B, C and D join
1.5 s apart. The script samples each shard's owner from `pg_locks` every 25 ms.
Every join recomputes the assignments, so a shard that went to B on its join
moves on to C or D on the next join. Each extra move drains and reloads that
shard's entities again during a single scale-out. Effect has no setting to
prevent this. Expected: each shard moves at most once. Actual (release and main): 18 shards
moved, and 7 of them moved more than once. Shards 9 and 22 went
`A->B->C->D`, and shards 6, 11, 14, 18 and 19 moved twice.

**6. Shard-move pause after SIGTERM.** `src/issue6-shard-move-pause.ts`.
Runners A and B split 4 shards. On A, entity `busy` runs a 60 s handler on one
shard. Entity `idle`, on another shard, has handled one `Ping`. A receives
SIGTERM, and the client immediately pings `idle` again. Results with
`entityTerminationTimeout` at 2 s and 8 s:

| build | timeout | idle shard released / acquired by B | busy shard released / acquired by B | Ping to the idle entity waited |
| --- | --- | --- | --- | --- |
| release | 2 s | 0.45 s / 0.74 s | 2.01 s / 2.75 s | 0.74 s |
| release | 8 s | 0.51 s / 0.77 s | 8.02 s / 8.82 s | 0.77 s |
| main | 2 s | 0.45 s / 0.76 s | 2.03 s / 2.76 s | 0.74 s |
| main | 8 s | 0.51 s / 0.80 s | 8.01 s / 8.84 s | 0.79 s |

The release already releases the idle shard promptly, so a message to an idle
entity on a moved shard waits under 1 s at either timeout. Only the shard with
the in-flight handler is held, for the full `entityTerminationTimeout`, and
every entity on that shard waits with it. The script's criterion is a Ping
under 1.5 s at both timeouts, and it passes. It reports the busy shard's pause,
which equals the timeout, but does not judge it. Main measures the same.
