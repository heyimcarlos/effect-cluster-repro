// Issue 6: shard-move pause. Does the old owner hold a shard until its
// in-flight handlers finish, up to entityTerminationTimeout?
//
// Runners A and B split 4 shards. On one of A's shards an entity runs a long
// handler (60 s, longer than the timeout); on A's other shard an entity is
// idle (it handled one Ping). A gets SIGTERM, and the client immediately
// sends the idle entity another Ping. pg_locks is sampled every 20 ms to time
// when A releases each shard and when B acquires it. This runs once per
// entityTerminationTimeout in TIMEOUTS (default "2,8" seconds).
import { Effect } from "effect"
import { EntityId, Sharding } from "effect/cluster"
import { Probe } from "./cluster.ts"
import { baseConfig, formatOwners, lockOwners, log, main, makeClient, resetDatabase, signal, sleep, startRunner, verdict, waitFor, waitForLine, waitForShards } from "./lib.ts"

const timeouts = (process.env.TIMEOUTS ?? "2,8").split(",").map(Number)

interface Measured {
  readonly timeout: number
  readonly idleShard: number
  readonly busyShard: number
  readonly release: Map<number, number>
  readonly acquire: Map<number, number>
  readonly pingWait: number
  readonly pingRunner: string
}

const round = async (timeout: number): Promise<Measured> => {
  log(`----- entityTerminationTimeout = ${timeout} s`)
  await resetDatabase()
  const config = { ...baseConfig, shardsPerGroup: 4, entityTerminationTimeout: `${timeout} seconds` }
  const a = startRunner("A", 34461, config)
  await waitForShards(a, 4)
  startRunner("B", 34462, config)
  const owners = (await waitFor("A and B to hold 2 shards each", async () => {
    const o = await lockOwners()
    return /^A: \d+,\d+ \| B: \d+,\d+$/.test(formatOwners(o)) && o
  }, 30_000))!
  log(`before SIGTERM: ${formatOwners(owners)}`)
  const aShards = [...owners].filter(([, r]) => r === "A").map(([s]) => s)

  const client = makeClient(config)
  const shardOf = (id: string) =>
    client.runPromise(Effect.map(Sharding.Sharding, (s) => s.getShardId(EntityId.make(id), "default").id))
  const pick = async (prefix: string, shard: number) => {
    for (let i = 0;; i++) if ((await shardOf(`${prefix}-${i}`)) === shard) return `${prefix}-${i}`
  }
  const [busyShard, idleShard] = aShards
  const busy = await pick("busy", busyShard)
  const idle = await pick("idle", idleShard)
  log(`busy entity ${busy} on shard ${busyShard}, idle entity ${idle} on shard ${idleShard}`)

  // Not awaited: the handler outlives this round.
  client.runPromise(Effect.flatMap(Probe.client, (make) => make(busy).Work())).catch(() => {})
  await waitForLine(a, /EVENT entered Work/)
  const ping = () => client.runPromise(Effect.flatMap(Probe.client, (make) => make(idle).Ping()))
  log(`first Ping handled by ${(await ping()).runner}; ${idle} is now resident and idle on A`)

  const release = new Map<number, number>()
  const acquire = new Map<number, number>()
  let sampling = true
  const t0 = Date.now()
  const sampler = (async () => {
    while (sampling) {
      const now = await lockOwners()
      for (const shard of aShards) {
        const owner = now.get(shard)
        if (owner !== "A" && !release.has(shard)) {
          release.set(shard, Date.now() - t0)
          log(`shard ${shard} released by A at +${Date.now() - t0} ms after SIGTERM`)
        }
        if (owner === "B" && !acquire.has(shard)) {
          acquire.set(shard, Date.now() - t0)
          log(`shard ${shard} acquired by B at +${Date.now() - t0} ms after SIGTERM`)
        }
      }
      await sleep(20)
    }
  })()

  signal(a, "SIGTERM")
  const sentAt = Date.now()
  const pong = await ping()
  const pingWait = pong.handledAt - sentAt
  log(`Ping sent at +${sentAt - t0} ms, handled by ${pong.runner} after ${pingWait} ms`)
  await waitFor("B to hold all 4 shards", () => acquire.size === aShards.length, 60_000)
  log(`runner A exited with code ${await a.exited} at +${Date.now() - t0} ms`)
  sampling = false
  await sampler
  await client.dispose()
  return { timeout, idleShard, busyShard, release, acquire, pingWait, pingRunner: pong.runner }
}

main(async () => {
  const results: Array<Measured> = []
  for (const timeout of timeouts) results.push(await round(timeout))
  const fmt = (ms: number | undefined) => (ms === undefined ? "never" : `${(ms / 1000).toFixed(2)} s`)
  const lines = results.map((r) =>
    `timeout ${r.timeout} s: idle shard ${r.idleShard} released ${fmt(r.release.get(r.idleShard))}, acquired by B ${
      fmt(r.acquire.get(r.idleShard))
    }; busy shard ${r.busyShard} released ${fmt(r.release.get(r.busyShard))}, acquired by B ${
      fmt(r.acquire.get(r.busyShard))
    }; Ping to the idle entity waited ${fmt(r.pingWait)} (handled by ${r.pingRunner})`
  )
  for (const line of lines) log(line)
  verdict(
    "Issue 6: shard-move pause after SIGTERM",
    "the idle entity does not wait for the busy one: its Ping is handled < 1.5 s after SIGTERM at every entityTerminationTimeout",
    lines.join("\n          "),
    results.every((r) => r.pingWait < 1500)
  )
})
