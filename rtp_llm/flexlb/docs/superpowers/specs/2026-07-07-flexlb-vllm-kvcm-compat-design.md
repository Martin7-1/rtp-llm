# FlexLB vLLM KVCM Compatibility Design

Date: 2026-07-07

## Context

FlexLB currently polls engine workers through gRPC:

- `GetWorkerStatus` updates worker liveness, queue/task state, and scheduling metrics.
- `GetCacheStatus` updates KV cache capacity and `cached_keys`, then `DefaultCacheAwareService` updates the local `KvCacheManager`.

The new vLLM gRPC server removes `GetCacheStatus`. It reports KV cache capacity through `GetWorkerStatus` instead:

- `available_kv_cache`
- `total_kv_cache`
- `block_size`

Block ownership is no longer pulled from workers. In the KVCM architecture, an external Meta Service subscribes to vLLM KV cache events and later exposes block owner lookup to FlexLB. That KVCM lookup is a phase-two implementation.

Inputs reviewed:

- DingTalk design document for vLLM Master gRPC Server.
- Local vLLM interface note: `/Users/zhengyi/Documents/code/working-projects/vllm-github/2026-07-06-vllm-master-grpc-server.md`.
- Existing FlexLB KVCM design note: `docs/flexlb-kvcm-grpc-interface-zh.md`.
- Current FlexLB code paths in `EngineSyncRunner`, `GrpcWorkerStatusRunner`, `GrpcCacheStatusCheckRunner`, `EngineStatusConverter`, `WorkerStatus`, and `CacheAwareService`.

## Goals

1. Keep compatibility with existing RTP-LLM style workers that still support `GetCacheStatus`.
2. Support new vLLM workers that expose KV capacity fields through `GetWorkerStatus`.
3. Add a `KVCM_ENABLE` switch so FlexLB stops calling worker `GetCacheStatus` when KVCM mode is enabled.
4. Pre-create a `KvcmCacheAwareService` Spring bean path for phase two without implementing KVCM gRPC lookup yet.

## Non-Goals

- Do not implement the KVCM `LookupBlocks` gRPC client in phase one.
- Do not compute prefix matches from KVCM responses in phase one.
- Do not remove the existing local `KvCacheManager` or `DefaultCacheAwareService`.
- Do not change routing strategies beyond their existing behavior when cache match results are empty.

## Approach

Use Spring Boot `@ConditionalOnProperty` for bean selection.

The environment variable `KVCM_ENABLE=true` maps to Spring property `kvcm.enable=true`. With this property:

- `KvcmCacheAwareService` becomes the active `CacheAwareService`.
- `DefaultCacheAwareService` is disabled.
- `EngineSyncRunner` skips submitting `GrpcCacheStatusCheckRunner`.
- `GrpcWorkerStatusRunner` continues polling `GetWorkerStatus` and uses embedded KV capacity fields when present.

Without this property:

- Existing behavior remains unchanged.
- `DefaultCacheAwareService` remains active.
- FlexLB continues polling `GetCacheStatus` and updating local cache indexes.

## Proto Compatibility

Extend `WorkerStatusPB` with append-only fields:

```proto
message WorkerStatusPB {
    string role = 1;
    int32 available_concurrency = 2;
    repeated TaskInfoPB running_task_info = 3;
    repeated TaskInfoPB finished_task_list = 4;
    int32 waiting_query_len = 5;
    int32 running_query_len = 6;
    double step_latency_ms = 7;
    int32 iterate_count = 8;
    int32 dp_size = 9;
    int32 tp_size = 10;
    int64 status_version = 12;
    bool alive = 13;
    string precision = 14;
    int64 latest_finished_version = 15;
    int64 available_kv_cache = 16;
    int64 total_kv_cache = 17;
    int64 block_size = 18;
}
```

FlexLB generates Java gRPC sources from `../cpp/model_rpc/proto/model_rpc_service.proto`, so this source proto must be updated before regenerating Java code.

The fields are append-only and default to `0` when an older server does not send them.

## Worker Status Data Flow

`EngineStatusConverter.convertToWorkerStatusResponse(...)` will map the new fields into `WorkerStatusResponse.cacheStatus` when useful:

- `availableKvCache = workerStatusPB.available_kv_cache`
- `totalKvCache = workerStatusPB.total_kv_cache`
- `blockSize = workerStatusPB.block_size`
- `version = -1`, because `CacheStatus.version` represents the old `GetCacheStatus` cache snapshot version, not `WorkerStatusPB.status_version`

`GrpcWorkerStatusRunner` will update `WorkerStatus` capacity from this `cacheStatus` only when it is valid:

- `totalKvCache > 0`
- `blockSize > 0`

When valid, it will:

- Set `workerStatus.cacheStatus`.
- Update `workerStatus.availableKvCacheTokens`.
- Update `workerStatus.usedKvCacheTokens` as `totalKvCache - availableKvCache`.

When fields are absent or default to `0`, it leaves existing cache status handling to `GrpcCacheStatusCheckRunner`.

## KVCM CacheAwareService

Add `KvcmCacheAwareService implements CacheAwareService`:

- Active only when `kvcm.enable=true`.
- `findMatchingEngines(...)` returns an empty map in phase one.
- `updateEngineBlockCache(...)` returns a success no-op result.

This makes cache-aware routing degrade to zero prefix hit in KVCM mode until phase two adds real `LookupBlocks` support. The service boundary remains useful because phase two can implement KVCM lookup behind the same `CacheAwareService` interface without touching the routing strategies first.

## Cache Status Polling Switch

`EngineSyncRunner` will check whether KVCM is enabled before submitting `GrpcCacheStatusCheckRunner`.

If KVCM is enabled:

- Submit `GrpcWorkerStatusRunner`.
- Do not submit `GrpcCacheStatusCheckRunner`.
- Ensure `cacheCheckInProgress` is not left stuck.

If KVCM is disabled:

- Keep current worker status and cache status polling behavior.

## Error Handling

- Missing `GetWorkerStatus` KV fields are treated as old-server compatibility, not an error.
- Invalid `GetWorkerStatus` KV fields (`totalKvCache <= 0` or `blockSize <= 0`) are ignored for cache capacity updates.
- KVCM mode no-op matching should not fail requests. It returns empty matches and lets existing routing score with zero cache hit.
- Existing `GetCacheStatus` errors remain handled by `GrpcCacheStatusCheckRunner` when KVCM is disabled.

## Testing

Add focused tests:

- `EngineStatusConverterTest`: `WorkerStatusPB` KV capacity fields become `WorkerStatusResponse.cacheStatus`.
- `GrpcWorkerStatusRunnerTest`: valid worker-status KV capacity updates `WorkerStatus.cacheStatus`, available KV tokens, and used KV tokens.
- `EngineSyncRunnerTest`: with KVCM enabled, worker status runner is submitted and cache status runner is not.
- `KvcmCacheAwareServiceTest`: phase-one no-op returns empty match results and success update result.

Run the targeted Maven tests first, then run the relevant module test suite if practical.

## Open Implementation Note

Because `flexlb-grpc` reads proto from `../cpp/model_rpc/proto/model_rpc_service.proto`, editing that source file may require filesystem approval outside the current `flexlb` workspace root.
