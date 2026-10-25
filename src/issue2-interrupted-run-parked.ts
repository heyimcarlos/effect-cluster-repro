// Issue 2: a run interrupted at shutdown stays Suspended forever.
//
// The workflow races its first activity ("step") against an external
// deferred. On runner A the body pauses until A is shutting down
// (Sharding.isShutdown), then starts the activity: the runner got SIGTERM
// while starting the run's first activity. Nothing completes the deferred and
// there is no clock, so only the handoff to runner B can finish the run.
import { Effect } from "effect"
import { Control, StepWorkflow } from "./cluster.ts"
import { baseConfig, entityMessages, log, main, makeClient, query, resetDatabase, signal, startRunner, verdict, waitFor, waitForLine, waitForShards } from "./lib.ts"

const config = { ...baseConfig, shardsPerGroup: 2, entityTerminationTimeout: "2 seconds" }
const waitSeconds = Number(process.env.WAIT_SECONDS ?? 20)

const counts = async (executionId: string) => {
  const [row] = await query<{ unprocessed: number; replied: number }>(
    `SELECT count(*) FILTER (WHERE NOT m.processed)::int AS unprocessed,
            count(*) FILTER (WHERE EXISTS (SELECT 1 FROM cluster_replies r WHERE r.request_id = m.id))::int AS replied
     FROM cluster_messages m WHERE m.entity_id = $1`,
    [executionId]
  )
  return `replied=${row.replied} unprocessed=${row.unprocessed}`
}

const rows = async (executionId: string) =>
  JSON.stringify((await entityMessages(executionId)).map((r) => [r.message_id, `processed=${r.processed}`, r.reply]))

main(async () => {
  await resetDatabase()
  const client = makeClient(config)
  const control = <A>(f: (c: any) => Effect.Effect<A, any, any>) =>
    client.runPromise(Effect.flatMap(Control.client, (make) => f(make("control"))))
  const executionId = await client.runPromise(StepWorkflow.executionId({ id: "step-1" }))
  const poll = () => control<string>((c) => c.Poll({ workflow: "HandoffStep", executionId }))
  log(`executionId=${executionId}`)

  const a = startRunner("A", 34431, config, { HOLD: "before-step" })
  await waitForShards(a, 2)
  await control((c) => c.Execute({ workflow: "HandoffStep", id: "step-1" }))
  await waitForLine(a, /EVENT reached before-step/)
  signal(a, "SIGTERM")
  log(`runner A exited with code ${await a.exited}`)
  log(`storage after A stopped: ${await counts(executionId)} ${await rows(executionId)}`)

  const b = startRunner("B", 34432, config)
  await waitForShards(b, 2)
  const startedAt = Date.now()
  const result = await waitFor(`the run to complete on B within ${waitSeconds} s`, async () => {
    const r = await poll()
    return r.startsWith("Complete") && r
  }, waitSeconds * 1000, 500)
  log(`storage at the end: ${await counts(executionId)} ${await rows(executionId)}`)

  verdict(
    "Issue 2: a run interrupted at shutdown stays Suspended forever",
    `the run completes on runner B (Complete("done"), activity "step" runs once)`,
    result
      ? `${result} on B after ${((Date.now() - startedAt) / 1000).toFixed(1)} s`
      : `still ${await poll()} after ${waitSeconds} s on B; ${await counts(executionId)}`,
    result === `Complete("done")`
  )
  await client.dispose()
})
