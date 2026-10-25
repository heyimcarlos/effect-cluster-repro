// Helpers for the issue scripts: a fresh database, runner child processes,
// Postgres queries (shard advisory locks, message rows) and the verdict.
import { Layer, ManagedRuntime } from "effect"
import { type ChildProcess, spawn } from "node:child_process"
import { mkdtempSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import pg from "pg"
import { clientLayer, type Config, DB_URL, logging } from "./cluster.ts"

const t0 = Date.now()
export const elapsed = () => Date.now() - t0
const stamp = () => `${new Date().toISOString()} +${(elapsed() / 1000).toFixed(3)}s`
export const log = (message: string) => console.log(`${stamp()} [repro] ${message}`)
export const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

/** Fast lock and assignment timings so each script runs in seconds. */
export const baseConfig: Config = {
  shardsPerGroup: 12,
  shardLockRefreshInterval: "1 second",
  shardLockExpiration: "5 seconds",
  refreshAssignmentsInterval: "500 millis",
  runnerHealthCheckInterval: "2 seconds",
  entityMessagePollInterval: "1 second",
  entityTerminationTimeout: "5 seconds"
}

/** Directory for the file gates that runner handlers wait on. */
export const gates = mkdtempSync(join(tmpdir(), "cluster-repro-"))
export const openGate = (name: string) => writeFileSync(join(gates, name), "")

export let db: pg.Pool | undefined

/** Kills any runner left from an earlier round, drops and recreates the
 * database, then connects to it. */
export const resetDatabase = async () => {
  await killRunners()
  await db?.end()
  const url = new URL(DB_URL)
  const name = url.pathname.slice(1)
  const admin = new pg.Client({ connectionString: Object.assign(new URL(DB_URL), { pathname: "/postgres" }).href })
  await admin.connect()
  await admin.query(`DROP DATABASE IF EXISTS "${name}" WITH (FORCE)`)
  await admin.query(`CREATE DATABASE "${name}"`)
  await admin.end()
  db = new pg.Pool({ connectionString: DB_URL, max: 4, application_name: "repro" })
}

export const query = async <A = Record<string, unknown>>(text: string, values: Array<unknown> = []) =>
  (await db!.query(text, values)).rows as Array<A>

export interface Runner {
  readonly name: string
  readonly proc: ChildProcess
  readonly lines: Array<string>
  readonly exited: Promise<number | null>
}

const runners: Array<Runner> = []

const killRunners = async () => {
  for (const runner of runners.splice(0)) {
    runner.proc.kill("SIGCONT")
    runner.proc.kill("SIGKILL")
    await runner.exited
  }
}
const quiet = /Runner is healthy|Starting singleton|Stopping singleton/

/** Starts a runner process; its stdout is printed with a timestamp prefix. */
export const startRunner = (
  name: string,
  port: number,
  config: Config,
  env: Record<string, string> = {}
): Runner => {
  const proc = spawn(process.execPath, [join(import.meta.dirname, "runner.ts")], {
    env: { ...process.env, ...env, RUNNER: name, PORT: String(port), CONFIG: JSON.stringify(config), GATES: gates },
    stdio: ["ignore", "pipe", "pipe"]
  })
  const lines: Array<string> = []
  const onData = (chunk: Buffer) => {
    for (const line of chunk.toString().split("\n")) {
      if (line.trim() === "") continue
      lines.push(line)
      // The prefix carries the time; drop logfmt's own timestamp and fiber id.
      if (!quiet.test(line)) {
        console.log(`${stamp()} [${name}] ${line.replace(/^timestamp=\S+ /, "").replace(/ fiber=#\d+/, "")}`)
      }
    }
  }
  proc.stdout!.on("data", onData)
  proc.stderr!.on("data", onData)
  const exited = new Promise<number | null>((resolve) => proc.on("exit", (code) => resolve(code)))
  const runner = { name, proc, lines, exited }
  runners.push(runner)
  log(`started runner ${name} pid=${proc.pid} port=${port}`)
  return runner
}

export const signal = (runner: Runner, sig: NodeJS.Signals) => {
  log(`${sig} -> runner ${runner.name}`)
  runner.proc.kill(sig)
}

/** Polls `check` until it returns a value other than false/undefined. */
export const waitFor = async <A>(
  label: string,
  check: () => Promise<A | false | undefined> | A | false | undefined,
  timeoutMs: number,
  intervalMs = 50
): Promise<A | undefined> => {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    const result = await check()
    if (result !== false && result !== undefined) return result
    await sleep(intervalMs)
  }
  log(`timed out after ${timeoutMs} ms waiting for: ${label}`)
  return undefined
}

export const waitForLine = (runner: Runner, pattern: RegExp, timeoutMs = 30_000) =>
  waitFor(`${runner.name}: ${pattern}`, () => runner.lines.some((line) => pattern.test(line)), timeoutMs)

/**
 * Shard advisory locks by owner, from pg_locks. SqlRunnerStorage takes
 * pg_try_advisory_lock(namespace, 1000000 + shard) for group "default" on a
 * reserved session whose application_name is `runner-<name>`.
 */
export const lockOwners = async () => {
  const rows = await query<{ runner: string; shard: number; pid: number }>(
    `SELECT replace(a.application_name, 'runner-', '') AS runner, (l.objid::bigint - 1000000)::int AS shard, l.pid
     FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
     WHERE l.locktype = 'advisory' AND l.granted AND l.objsubid = 2
     ORDER BY shard`
  )
  return new Map(rows.map((row) => [row.shard, row.runner]))
}

/** "A: 1,2,3 | B: 4,5" */
export const formatOwners = (owners: Map<number, string>) => {
  const byRunner = new Map<string, Array<number>>()
  for (const [shard, runner] of owners) byRunner.set(runner, [...(byRunner.get(runner) ?? []), shard])
  return [...byRunner].sort().map(([runner, shards]) => `${runner}: ${shards.join(",")}`).join(" | ") || "(none)"
}

export const waitForShards = (runner: Runner, count: number, timeoutMs = 30_000) =>
  waitFor(`runner ${runner.name} to hold ${count} shard locks`, async () => {
    const owners = await lockOwners()
    return [...owners.values()].filter((owner) => owner === runner.name).length === count && owners
  }, timeoutMs)

/** A client-only cluster node in this process, for sending messages. */
export const makeClient = (config: Config) =>
  ManagedRuntime.make(clientLayer(config).pipe(Layer.provide(logging("Warn"))))

/** Message rows of one entity, with their latest reply. */
export const entityMessages = (entityId: string) =>
  query(
    `SELECT m.id::text, m.message_id, m.tag, m.processed, m.payload,
            (SELECT r.payload FROM cluster_replies r WHERE r.request_id = m.id ORDER BY r.id DESC LIMIT 1) AS reply
     FROM cluster_messages m WHERE m.entity_id = $1 ORDER BY m.id`,
    [entityId]
  )

/** Prints expected and actual, then PASS or FAIL. The process exits 1 on FAIL. */
export const verdict = (title: string, expected: string, actual: string, pass: boolean) => {
  console.log(`\n=== ${title}`)
  console.log(`expected: ${expected}`)
  console.log(`actual:   ${actual}`)
  console.log(pass ? "PASS (bug not reproduced)" : "FAIL (bug reproduced)")
  process.exitCode = pass ? 0 : 1
}

/** Runs a script body, then kills every runner and closes connections. */
export const main = (body: () => Promise<void>) =>
  body().catch((error) => {
    console.error(error)
    process.exitCode = 2
  }).finally(async () => {
    await killRunners()
    await db?.end()
    process.exit()
  })
