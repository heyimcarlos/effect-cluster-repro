// Issue 5: runners joining one at a time move shards more than once.
//
// Runner A owns 24 shards; B, C and D join 1.5 s apart. Every shard's owner
// is sampled from pg_locks every 25 ms; a move is a change of the runner
// holding the shard's advisory lock. Effect has no setting for this.
import { baseConfig, formatOwners, lockOwners, log, main, type Runner, resetDatabase, sleep, startRunner, verdict, waitFor, waitForShards } from "./lib.ts"

const shards = 24
const config = { ...baseConfig, shardsPerGroup: shards }

main(async () => {
  await resetDatabase()
  const a = startRunner("A", 34451, config)
  const initial = (await waitForShards(a, shards))!
  log(`initial: ${formatOwners(initial)}`)

  const owners = new Map(initial)
  const moves = new Map<number, Array<string>>()
  let sampling = true
  const sampler = (async () => {
    while (sampling) {
      for (const [shard, owner] of await lockOwners()) {
        const previous = owners.get(shard)
        if (owner === previous) continue
        owners.set(shard, owner)
        moves.set(shard, [...(moves.get(shard) ?? []), `${previous}->${owner}`])
        log(`shard ${shard} moved ${previous} -> ${owner}`)
      }
      await sleep(25)
    }
  })()

  const joined: Array<Runner> = []
  for (const [name, port] of [["B", 34452], ["C", 34453], ["D", 34454]] as const) {
    joined.push(startRunner(name, port, config))
    await sleep(1500)
  }
  // Settled: each runner holds a quarter of the shards and nothing moves for 3 s.
  let stableSince = Date.now()
  let last = ""
  await waitFor("assignments to settle", async () => {
    const current = formatOwners(await lockOwners())
    if (current !== last) [last, stableSince] = [current, Date.now()]
    return [a, ...joined].every((r) => current.includes(`${r.name}: `)) && Date.now() - stableSince > 3000
  }, 60_000, 100)
  sampling = false
  await sampler
  log(`final: ${last}`)

  const movedTwice = [...moves].filter(([, path]) => path.length > 1)
  for (const [shard, path] of moves) log(`shard ${shard}: ${path.join(", ")}`)
  verdict(
    "Issue 5: runners joining one at a time move shards more than once",
    "each shard moves at most once",
    `${moves.size} shards moved, ${movedTwice.length} of them more than once (${
      movedTwice.map(([shard, path]) => `${shard}: ${path.join(" ")}`).join("; ") || "none"
    })`,
    moves.size > 0 && movedTwice.length === 0
  )
})
