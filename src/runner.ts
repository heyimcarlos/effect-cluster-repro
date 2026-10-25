// One cluster runner process. Started by the issue scripts through lib.ts:
// RUNNER (name), PORT, CONFIG (JSON ShardingConfig overrides), optional HOLD.
import { NodeRuntime } from "@effect/platform-node"
import { Layer } from "effect"
import { EntitiesLayer, logging, runnerLayer } from "./cluster.ts"

const name = process.env.RUNNER!
const port = Number(process.env.PORT)
const config = JSON.parse(process.env.CONFIG ?? "{}")

EntitiesLayer.pipe(
  Layer.provideMerge(runnerLayer(config, name, port)),
  Layer.provide(logging("Debug")),
  Layer.launch,
  NodeRuntime.runMain
)
