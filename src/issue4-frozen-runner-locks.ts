// Issue 4: a frozen runner's advisory-lock session never expires, so its
// shards never move.
//
// Two runners split the shards. Runner A is frozen with SIGSTOP and stays
// frozen for FREEZE_SECONDS (default 30, six lock expirations). Its heartbeat
// stops, so runner B drops it and assigns itself every shard, but A's
// advisory locks live on A's still-open Postgres session.
import { baseConfig, formatOwners, lockOwners, log, main, query, resetDatabase, signal, sleep, startRunner, verdict, waitForShards } from "./lib.ts"

const freezeSeconds = Number(process.env.FREEZE_SECONDS ?? 30)
const config = baseConfig
const shards = config.shardsPerGroup as number

main(async () => {
  await resetDatabase()
  const a = startRunner("A", 34431, config)
  await waitForShards(a, shards)
  const b = startRunner("B", 34432, config)
  const split = await waitForShards(b, shards / 2)
  log(`assignments before the freeze: ${formatOwners(split ?? await lockOwners())}`)

  signal(a, "SIGSTOP")
  const frozenAt = Date.now()
  let movedAfter: number | undefined
  while (Date.now() - frozenAt < freezeSeconds * 1000) {
    await sleep(1000)
    const owners = await lockOwners()
    const sessions = await query<{ runner: string; pid: number; locks: string; state: string }>(
      `SELECT replace(a.application_name, 'runner-', '') AS runner, l.pid, count(*)::text AS locks, a.state
       FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
       WHERE l.locktype = 'advisory' AND l.granted AND l.objsubid = 2
       GROUP BY 1, 2, 4 ORDER BY 1`
    )
    const held = sessions.map((s) => `${s.runner}(pid ${s.pid}, ${s.state}): ${s.locks} locks`).join(", ")
    log(`t=${((Date.now() - frozenAt) / 1000).toFixed(1)}s after SIGSTOP | ${formatOwners(owners)} | ${held}`)
    if (movedAfter === undefined && [...owners.values()].filter((o) => o === "B").length === shards) {
      movedAfter = Date.now() - frozenAt
    }
  }

  verdict(
    "Issue 4: a frozen runner's advisory locks never expire",
    `runner B holds all ${shards} shard locks about shardLockExpiration (${config.shardLockExpiration}) after A froze`,
    movedAfter === undefined
      ? `after ${freezeSeconds} s frozen, A's session still holds its locks; B holds ${
        [...(await lockOwners()).values()].filter((o) => o === "B").length
      }/${shards}`
      : `B held all ${shards} shards ${(movedAfter / 1000).toFixed(1)} s after the freeze`,
    movedAfter !== undefined
  )
})
