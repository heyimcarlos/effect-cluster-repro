# Effect Cluster reproductions

Five bugs in Effect Cluster on PostgreSQL, plus one measurement. Each has its
own script. Every script starts real runner processes that use
`NodeClusterSocket.layer`, `SqlMessageStorage` and `SqlRunnerStorage`, prints
the runners' logs, and ends with what it expected, what happened, and `PASS`
or `FAIL`. A script exits with 1 when it reproduces its bug.

All five bugs reproduce on effect 4.0.6 and on current `main`.

| # | Issue | 4.0.6 | main |
| --- | --- | --- | --- |
| 1 | A workflow replay picks a different winner for a race it already recorded | FAIL | FAIL |
| 2 | A workflow interrupted during shutdown stays Suspended forever | FAIL | FAIL |
| 3 | A runner that lost its shard can still write replies | FAIL | FAIL |
| 4 | A frozen runner keeps its shards forever | FAIL | FAIL |
| 5 | Runners that join one after another move the same shard several times | FAIL | FAIL |
| 6 | How long a shard pauses after SIGTERM | measured | measured |

## Run it

```sh
docker compose up -d          # PostgreSQL 17 on 127.0.0.1:55642
npm install
./run-all.sh                  # effect 4.0.6, logs in logs/stock/
scripts/build-main.sh         # clones and builds Effect main into .build/main, needs pnpm
./run-all.sh main             # logs in logs/main/
docker compose down -v
```

Run one script with `node src/issue3-stale-owner-writes.ts`. Node 24 runs the
`.ts` files directly.

Each script drops and recreates its database. The default is `repro` on the
Docker Postgres above. `REPRO_DATABASE_URL` points the scripts at another
database, and that database gets dropped too, so only use a scratch server.
The runners listen on ports 34431 to 34462.

The scripts use short timings so they finish fast. The shard lock expires
after 5 s, runners refresh it every 1 s and reread assignments every 500 ms.
They use 2 to 24 shards. All of this is in `baseConfig` in `src/lib.ts`.

Builds tested:

- **4.0.6.** `effect`, `@effect/sql-pg` and `@effect/platform-node` 4.0.6 from npm.
- **main.** Effect-TS/effect at `837e906d9a476bff12fbd063d1635f748e1de7de`, 2026-10-25, built by `scripts/build-main.sh`.

## The issues

### 1. A workflow replay picks a different winner for a race it already recorded

`src/issue1-race-replay.ts`

A workflow races an external deferred called `gate` against a one-hour
`DurableClock.sleep` with `DurableDeferred.raceAll`. If the gate wins it runs
activity `claim`. If the clock wins it runs `close`.

The race result is already stored as `idle`, the clock's answer. Then the
shard moves to runner B and the gate completes. B replays the workflow, and
`deferredResult` only reads processed replies, so B never sees the stored
result. It runs the race again, the gate wins this time, and B runs `claim`.
The workflow now has two different results for one race in its history and
ran the branch it should not have.

Expected `Complete("idle")` after running `close`. Got `Complete("text")`
after running `claim`.

The script writes the stored race row itself, as a copy of the engine's own
`gate` row. In real use, a runner leaves this row behind when it saves the
race result and stops before processing it. That timing is hard to hit on
demand, so the script sets it up directly.

### 2. A workflow interrupted during shutdown stays Suspended forever

`src/issue2-interrupted-run-parked.ts`

Runner A gets SIGTERM just as a workflow starts its first activity. The
script makes this happen every time: the workflow waits until
`Sharding.isShutdown` is true, then starts the activity.

Shutdown interrupts the activity. The engine reads that interruption as a
normal suspend and stores `Suspended` as the workflow's reply. Nothing will
ever wake it, since it is not waiting on a deferred or a clock. Runner B takes
the shard, sees the activity request, and answers it with `Suspended` as well.
The workflow is stuck.

Expected the workflow to finish on B with `Complete("done")`. Got `Suspended`
20 s after B took the shard. Any rolling deploy can strand workflows like this.

### 3. A runner that lost its shard can still write replies

`src/issue3-stale-owner-writes.ts`

Runner A starts a request whose handler waits on a gate. The script freezes A
with SIGSTOP and kills its Postgres lock connection with
`pg_terminate_backend`, the way a network partition would. Runner B takes
every shard and starts the same request.

Then A wakes up, its gate opens, and it finishes first. A only checks shard
ownership in memory, and storage does not check it at all, so storage accepts
A's reply. The client gets A's result and B's reply is dropped. Two runners
handled the same request, and the one that won no longer owned the shard.

Expected storage to reject A's reply and the client to get B's result. Got
`"A"`.

### 4. A frozen runner keeps its shards forever

`src/issue4-frozen-runner-locks.ts`

Two runners split 12 shards. The script freezes runner A with SIGSTOP for
30 s, six times the 5 s lock expiration.

About 5 s in, B marks A as unhealthy and assigns itself all 12 shards. It
never gets them. A's shard locks are Postgres advisory locks held by A's
database session, and that session stays open while A is frozen. The 5 s
lock expiration does not release them. They stay held until the session
ends. The script samples `pg_locks`
every second, and A still holds its 6 locks at the end.

Expected B to hold all 12 shards about 5 s after the freeze. Got 6 of 12
after 30 s. A paused VM, a long GC pause, or a host that loses its network
while its TCP connection stays up can block shards like this for as long as
the session lives.

### 5. Runners that join one after another move the same shard several times

`src/issue5-join-moves.ts`

Runner A holds 24 shards. Runners B, C and D join 1.5 s apart, about how a
platform starts new instances during a scale-out. The script checks each
shard's owner in `pg_locks` every 25 ms.

Every join reassigns the shards. A shard that moved to B when B joined moves
again to C or D when they join. Each move stops that shard's entities and
starts them again somewhere else, so one scale-out restarts them two or three
times. No setting prevents this.

Expected every shard to move at most once. Got 18 shards moved, 7 of them more
than once. Shards 9 and 22 went A to B to C to D.

### 6. How long a shard pauses after SIGTERM

`src/issue6-shard-move-pause.ts`

This one is a measurement, not a bug. The script passes on both builds.

Runners A and B split 4 shards. On A, entity `busy` runs a 60 s handler on one
shard, and entity `idle` sits on another shard. A gets SIGTERM, and the client
pings `idle` right away.

| build | `entityTerminationTimeout` | idle shard released, taken by B | busy shard released, taken by B | ping to `idle` waited |
| --- | --- | --- | --- | --- |
| 4.0.6 | 2 s | 0.45 s, 0.74 s | 2.01 s, 2.75 s | 0.74 s |
| 4.0.6 | 8 s | 0.51 s, 0.77 s | 8.02 s, 8.82 s | 0.77 s |
| main | 2 s | 0.45 s, 0.76 s | 2.03 s, 2.76 s | 0.74 s |
| main | 8 s | 0.51 s, 0.80 s | 8.01 s, 8.84 s | 0.79 s |

A shard with only idle entities moves in under a second. A shard with a
handler still running stays on A for the full `entityTerminationTimeout`, and
every other entity on that shard waits with it. The script only checks that
the ping to `idle` takes under 1.5 s. It reports the busy shard's pause but
does not judge it.
