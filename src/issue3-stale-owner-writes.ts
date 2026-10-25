// Issue 3: no write fencing, so a stale owner still writes after losing its
// shard.
//
// Runner A starts a persisted request whose handler waits on a gate. A is
// frozen (SIGSTOP) and its advisory-lock session is ended from the server
// (pg_terminate_backend), as a network partition would. Runner B acquires the
// shard and runs the same request, also waiting on its own gate. A is resumed
// with its gate open, so it finishes first; B's gate opens after that.
import { Effect } from "effect"
import { Hold, HoldRequest } from "./cluster.ts"
import {
  baseConfig,
  entityMessages,
  formatOwners,
  lockOwners,
  log,
  main,
  makeClient,
  openGate,
  query,
  resetDatabase,
  signal,
  sleep,
  startRunner,
  verdict,
  waitFor,
  waitForLine,
  waitForShards
} from "./lib.ts"

const config = { ...baseConfig, shardsPerGroup: 4 }

main(async () => {
  await resetDatabase()
  const a = startRunner("A", 34431, config)
  await waitForShards(a, 4)
  const client = makeClient(config)

  log("client sends the persisted request Hold(id=fenced) to entity 'held'")
  let result: string | undefined
  client.runPromise(Effect.flatMap(Hold.client, (make) => make("held").Hold(new HoldRequest({ id: "fenced" }))))
    .then((reply) => (result = reply))
  await waitForLine(a, /EVENT entered Hold/)

  signal(a, "SIGSTOP")
  const [{ pid }] = await query<{ pid: number }>(
    `SELECT DISTINCT l.pid FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
     WHERE l.locktype = 'advisory' AND a.application_name = 'runner-A'`
  )
  await query("SELECT pg_terminate_backend($1)", [pid])
  log(`terminated runner A's lock session (backend pid ${pid}); locks now: ${formatOwners(await lockOwners())}`)

  const b = startRunner("B", 34432, config)
  await waitForShards(b, 4)
  log(`locks: ${formatOwners(await lockOwners())}`)
  await waitForLine(b, /EVENT entered Hold/)

  log("open A's gate, then SIGCONT A: the stale owner finishes first")
  openGate("gate-A")
  signal(a, "SIGCONT")
  await waitForLine(a, /EVENT returning Hold/, 10_000)
  await sleep(3000)
  log(`rows after A finished: ${JSON.stringify(await entityMessages("held"))}`)

  log("open B's gate")
  openGate("gate-B")
  await waitFor("the client's reply", () => result, 20_000)
  await sleep(2000)
  const rows = await entityMessages("held")
  log(`rows at the end: ${JSON.stringify(rows)}`)
  const stored = rows.map((row) => String(row.reply ?? "")).find((r) => r !== "")
  const storedBy = stored?.match(/"(A|B)"/)?.[1] ?? "none"

  verdict(
    "Issue 3: a stale owner still writes after losing its shard",
    "A's late reply is rejected; the stored reply and the client's result come from B (the current owner)",
    `stored reply written by runner ${storedBy}; client got ${JSON.stringify(result)}`,
    storedBy === "B" && result === "B"
  )
  await client.dispose()
})
