// Entities, workflows and the cluster layer shared by the runner processes and
// the client in each issue script. Only public exports of effect,
// @effect/sql-pg and @effect/platform-node are used.
import { NodeClusterSocket } from "@effect/platform-node"
import { PgClient } from "@effect/sql-pg"
import { Effect, Layer, Logger, Option, PrimaryKey, Redacted, References, Schema } from "effect"
import { ClusterSchema, ClusterWorkflowEngine, Entity, RunnerAddress, Sharding } from "effect/cluster"
import { Rpc } from "effect/rpc"
import { Activity, DurableClock, DurableDeferred, Workflow } from "effect/workflow"
import { existsSync } from "node:fs"

export const DB_URL = process.env.REPRO_DATABASE_URL ?? "postgres://postgres:postgres@127.0.0.1:55642/repro"

const runnerName = process.env.RUNNER ?? "client"
const gates = process.env.GATES ?? "/tmp"

/** A line the issue scripts wait for on a runner's stdout. */
const event = (message: string) =>
  Effect.sync(() => console.log(`${new Date().toISOString()} EVENT ${message} runner=${runnerName}`))

const waitForFile = (name: string) =>
  Effect.gen(function*() {
    while (!existsSync(`${gates}/${name}`)) yield* Effect.sleep("20 millis")
  })

// --- Issue 3: a persisted request whose handler waits on a per-runner gate ---

export class HoldRequest extends Schema.Class<HoldRequest>("HoldRequest")({ id: Schema.String }) {
  [PrimaryKey.symbol]() {
    return this.id
  }
}

export const Hold = Entity.make("Hold", [
  Rpc.make("Hold", { payload: HoldRequest, success: Schema.String })
]).annotateRpcs(ClusterSchema.Persisted, true)

const HoldLayer = Hold.toLayer({
  Hold: ({ payload }) =>
    event(`entered Hold id=${payload.id}`).pipe(
      Effect.andThen(waitForFile(`gate-${runnerName}`)),
      Effect.andThen(event(`returning Hold id=${payload.id} reply=${runnerName}`)),
      Effect.as(runnerName)
    )
})

// --- Issue 6: a long in-flight handler and an idle entity ---

export const Probe = Entity.make("Probe", [
  Rpc.make("Work", { success: Schema.String }),
  Rpc.make("Ping", { success: Schema.Struct({ runner: Schema.String, handledAt: Schema.Number }) })
])

const busyMillis = Number(process.env.BUSY_MS ?? 60_000)

const ProbeLayer = Probe.toLayer({
  Work: () =>
    event("entered Work").pipe(
      Effect.andThen(Effect.sleep(busyMillis)),
      Effect.andThen(event("finished Work")),
      Effect.as(runnerName)
    ),
  Ping: () =>
    Effect.sync(() => ({ runner: runnerName, handledAt: Date.now() })).pipe(
      Effect.tap(() => event("handled Ping"))
    )
})

// --- Issues 1 and 2: workflows ---

/** The hold point this runner pauses at, if any (set per runner by a script). */
const hold = process.env.HOLD

export const Gate = DurableDeferred.make("gate", { success: Schema.String })
const Winner = Schema.Literals(["text", "idle"])

/** A durable race between an external deferred and a one-hour clock. */
export const RaceWorkflow = Workflow.make("HandoffRace", {
  payload: { id: Schema.String },
  success: Winner,
  idempotencyKey: ({ id }) => id
})

const RaceWorkflowLayer = RaceWorkflow.toLayer(({ id }) =>
  Effect.gen(function*() {
    if (hold === "before-race") {
      yield* event(`reached before-race id=${id}`)
      yield* waitForFile("release")
    }
    const winner = yield* DurableDeferred.raceAll({
      name: "wait",
      success: Winner,
      error: Schema.Never,
      effects: [
        DurableDeferred.await(Gate).pipe(Effect.as("text" as const)),
        Effect.as(DurableClock.sleep({ name: "idle", duration: "1 hour" }), "idle" as const)
      ]
    })
    const branch = winner === "text" ? "claim" : "close"
    yield* Activity.make({ name: branch, execute: event(`activity ${branch} id=${id}`) })
    return winner
  })
)

/** An activity raced against the external deferred. */
export const StepWorkflow = Workflow.make("HandoffStep", {
  payload: { id: Schema.String },
  success: Schema.String,
  idempotencyKey: ({ id }) => id
})

const StepWorkflowLayer = StepWorkflow.toLayer(({ id }) =>
  Effect.gen(function*() {
    if (hold === "before-step") {
      // Start the first activity only once this runner is shutting down.
      const sharding = yield* Sharding.Sharding
      yield* event(`reached before-step id=${id}`)
      while (!(yield* sharding.isShutdown)) yield* Effect.sleep("5 millis")
      yield* event(`runner is shutting down, starting activity id=${id}`)
    }
    return yield* DurableDeferred.raceAll({
      name: "steer",
      success: Schema.String,
      error: Schema.Never,
      effects: [
        Activity.make({
          name: "step",
          success: Schema.String,
          execute: Effect.as(event(`activity step id=${id}`), "done")
        }),
        Effect.as(DurableDeferred.await(Gate), "steered")
      ]
    })
  })
)

const WorkflowName = Schema.Literals(["HandoffRace", "HandoffStep"])

/** Starts workflows and reads their result from inside a runner. Not
 * persisted, so the client retries it until it routes. */
export const Control = Entity.make("Control", [
  Rpc.make("Execute", { payload: { workflow: WorkflowName, id: Schema.String } }),
  Rpc.make("OpenGate", { payload: { executionId: Schema.String } }),
  Rpc.make("Poll", { payload: { workflow: WorkflowName, executionId: Schema.String }, success: Schema.String })
])

const ControlLayer = Control.toLayer({
  Execute: ({ payload: { id, workflow } }) =>
    Effect.asVoid(
      workflow === "HandoffRace"
        ? RaceWorkflow.execute({ id }, { discard: true })
        : StepWorkflow.execute({ id }, { discard: true })
    ),
  OpenGate: ({ payload: { executionId } }) =>
    DurableDeferred.succeed(Gate, {
      token: DurableDeferred.tokenFromExecutionId(Gate, { workflow: RaceWorkflow, executionId }),
      value: "hello"
    }),
  Poll: ({ payload: { executionId, workflow } }) =>
    Effect.map(
      workflow === "HandoffRace" ? RaceWorkflow.poll(executionId) : StepWorkflow.poll(executionId),
      (result) => {
        if (Option.isNone(result)) return "none"
        if (result.value._tag === "Suspended") return "Suspended"
        const exit = result.value.exit
        return exit._tag === "Success" ? `Complete(${JSON.stringify(exit.value)})` : "Complete(Failure)"
      }
    )
})

export const EntitiesLayer = Layer.mergeAll(
  HoldLayer,
  ProbeLayer,
  ControlLayer,
  RaceWorkflowLayer,
  StepWorkflowLayer
).pipe(Layer.provide(ClusterWorkflowEngine.layer), Layer.orDie)

// --- Cluster layers ---

export type Config = Record<string, unknown>

const pg = (applicationName: string) => PgClient.layer({ url: Redacted.make(DB_URL), applicationName })

export const logging = (level: "Debug" | "Info" | "Warn") =>
  Layer.mergeAll(Logger.layer([Logger.consoleLogFmt]), Layer.succeed(References.MinimumLogLevel, level))

/** A socket runner on 127.0.0.1:`port` with SQL message and runner storage. */
export const runnerLayer = (config: Config, name: string, port: number) =>
  NodeClusterSocket.layer({
    shardingConfig: { ...config, runnerAddress: Option.some(RunnerAddress.make("127.0.0.1", port)) }
  }).pipe(Layer.provide(pg(`runner-${name}`)))

/** A client-only node with the same storage, used by the issue scripts. */
export const clientLayer = (config: Config) =>
  NodeClusterSocket.layer({ clientOnly: true, shardingConfig: config }).pipe(Layer.provide(pg("client")))
