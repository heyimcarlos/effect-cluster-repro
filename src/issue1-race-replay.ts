// Issue 1: a workflow replay decides a recorded race again after a shard
// handoff.
//
// The workflow runs DurableDeferred.raceAll over an external deferred ("gate")
// and a one-hour DurableClock, then runs activity "claim" (gate won) or
// "close" (clock won).
//
// 1. Runner A starts the run; it suspends in the race. A stops (SIGTERM).
// 2. Runner B takes the shards. The gate is completed, which wakes the run on
//    B; B's replay pauses before the race (a file gate in the workflow body).
// 3. The script stores the race result "idle" the way the engine stores it:
//    a "deferred" request named raceAll/wait, copied from the engine's own
//    gate row. It stands for the previous owner having recorded the clock
//    result while it drained, with that request persisted but not yet
//    processed. The row is written directly because the timing that produces
//    it naturally (the clock firing during a handoff) is hard to hit on demand.
//    B reads storage only when notified or every entityMessagePollInterval
//    (one minute here), so it has not processed the row.
// 4. B's replay continues. The stored race result is "idle", so the run should
//    take it and run "close".
import { Effect } from "effect"
import { MachineId, Snowflake } from "effect/cluster"
import { Control, RaceWorkflow } from "./cluster.ts"
import { baseConfig, entityMessages, log, main, makeClient, openGate, query, resetDatabase, signal, startRunner, verdict, waitFor, waitForLine, waitForShards } from "./lib.ts"

const config = { ...baseConfig, shardsPerGroup: 2, entityMessagePollInterval: "1 minute", entityTerminationTimeout: "2 seconds" }

main(async () => {
  await resetDatabase()
  const client = makeClient(config)
  const control = <A>(f: (c: any) => Effect.Effect<A, any, any>) =>
    client.runPromise(Effect.flatMap(Control.client, (make) => f(make("control"))))
  const executionId = await client.runPromise(RaceWorkflow.executionId({ id: "race-1" }))
  const poll = () => control<string>((c) => c.Poll({ workflow: "HandoffRace", executionId }))
  log(`executionId=${executionId}`)

  const a = startRunner("A", 34431, config)
  await waitForShards(a, 2)
  await control((c) => c.Execute({ workflow: "HandoffRace", id: "race-1" }))
  await waitFor("the run to suspend on A", async () => (await poll()) === "Suspended", 20_000, 200)
  log("run is Suspended on A (waiting for the gate or the one-hour clock)")
  signal(a, "SIGTERM")
  log(`runner A exited with code ${await a.exited}`)

  const b = startRunner("B", 34432, config, { HOLD: "before-race" })
  await waitForShards(b, 2)
  log("complete the gate deferred")
  await control((c) => c.OpenGate({ executionId }))
  await waitForLine(b, /EVENT reached before-race/)

  const prefix = `Workflow/HandoffRace/${executionId}/deferred`
  const [gate] = await query<{ payload: string }>("SELECT payload FROM cluster_messages WHERE message_id = $1", [`${prefix}/gate`])
  const payload = JSON.parse(gate.payload)
  const id = Snowflake.make({ machineId: MachineId.make(1023), sequence: 0, timestamp: Date.now() }).toString()
  await query(
    `INSERT INTO cluster_messages (id, message_id, shard_id, entity_type, entity_id, kind, tag, payload, headers,
       trace_id, span_id, sampled, request_id, reply_id, deliver_at)
     SELECT $1, $2, shard_id, entity_type, entity_id, kind, tag, $3, headers, trace_id, span_id, sampled, $1,
       reply_id, deliver_at
     FROM cluster_messages WHERE message_id = $4`,
    [id, `${prefix}/raceAll/wait`, JSON.stringify({ ...payload, name: "raceAll/wait", exit: { ...payload.exit, value: "idle" } }), `${prefix}/gate`]
  )
  log(`stored raceAll/wait = "idle" as an unprocessed deferred request (id ${id})`)
  log(`rows: ${JSON.stringify((await entityMessages(executionId)).map((r) => [r.message_id, r.processed]))}`)

  openGate("release")
  const result = await waitFor("the run to complete", async () => {
    const r = await poll()
    return r.startsWith("Complete") && r
  }, 20_000, 200)
  const activities = (await entityMessages(executionId))
    .filter((r) => r.tag === "activity")
    .map((r) => String(r.message_id).split("/activity/")[1])
  log(`final rows: ${JSON.stringify((await entityMessages(executionId)).map((r) => [r.message_id, r.processed, r.reply]))}`)

  verdict(
    "Issue 1: a replay decides a recorded race again after a shard handoff",
    `result Complete("idle"), activities [close/1] (the stored race result)`,
    `result ${result ?? "not complete"}, activities [${activities.join(", ")}]`,
    result === `Complete("idle")` && activities.join() === "close/1"
  )
  await client.dispose()
})
