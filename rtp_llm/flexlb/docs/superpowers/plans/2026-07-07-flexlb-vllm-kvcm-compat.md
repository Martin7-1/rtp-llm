# FlexLB vLLM KVCM Compatibility Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make FlexLB compatible with vLLM workers that expose KV cache capacity through `GetWorkerStatus`, while pre-wiring a KVCM `CacheAwareService` path behind `KVCM_ENABLE=true`.

**Architecture:** Extend the shared engine proto append-only, map new worker-status KV fields into existing `CacheStatus`, and update `WorkerStatus` from that cache status when present. Use Spring Boot `@ConditionalOnProperty(prefix = "kvcm", name = "enable")` to switch between the existing local cache-aware service and a phase-one no-op KVCM service. Pass the same property into `EngineSyncRunner` so KVCM mode skips `GetCacheStatus`.

**Tech Stack:** Java 21, Spring Boot conditional beans, Maven, gRPC/protobuf generated Java, JUnit 5, Mockito.

---

## File Structure

- Modify: `../cpp/model_rpc/proto/model_rpc_service.proto`
  - Source proto used by `flexlb-grpc` during `generate-sources`.
- Modify: `flexlb-cache/src/main/java/org/flexlb/cache/service/impl/DefaultCacheAwareService.java`
  - Active only when `kvcm.enable=false` or missing.
- Create: `flexlb-cache/src/main/java/org/flexlb/cache/service/impl/KvcmCacheAwareService.java`
  - Phase-one KVCM `CacheAwareService` implementation.
- Test: `flexlb-cache/src/test/java/org/flexlb/cache/service/impl/KvcmCacheAwareServiceTest.java`
  - Verifies no-op KVCM behavior.
- Modify: `flexlb-sync/src/main/java/org/flexlb/service/grpc/EngineStatusConverter.java`
  - Converts new `WorkerStatusPB` KV fields to `WorkerStatusResponse.cacheStatus`.
- Test: `flexlb-sync/src/test/java/org/flexlb/service/grpc/EngineStatusConverterTest.java`
  - Verifies conversion.
- Modify: `flexlb-sync/src/main/java/org/flexlb/sync/runner/GrpcWorkerStatusRunner.java`
  - Applies worker-status KV capacity updates to `WorkerStatus`.
- Test: `flexlb-sync/src/test/java/org/flexlb/sync/runner/GrpcWorkerStatusCheckRunnerTest.java`
  - Extends existing test to assert KV capacity update.
- Modify: `flexlb-sync/src/main/java/org/flexlb/sync/runner/EngineSyncRunner.java`
  - Skips cache status runner when KVCM is enabled.
- Modify: `flexlb-sync/src/main/java/org/flexlb/sync/synchronizer/MasterEngineSynchronizer.java`
  - Reads `kvcm.enable` Spring property and passes it to `EngineSyncRunner`.
- Test: `flexlb-sync/src/test/java/org/flexlb/sync/runner/EngineSyncRunnerTest.java`
  - Verifies cache runner submission behavior with KVCM enabled.

### Task 1: Extend WorkerStatusPB Proto

**Files:**
- Modify: `../cpp/model_rpc/proto/model_rpc_service.proto`
- Generated during test/build: `flexlb-grpc/target/generated-sources/protobuf/**`

- [ ] **Step 1: Update the source proto**

In `../cpp/model_rpc/proto/model_rpc_service.proto`, append fields to `WorkerStatusPB`:

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

- [ ] **Step 2: Regenerate gRPC sources**

Run:

```bash
./mvnw -pl flexlb-grpc -am generate-sources
```

Expected: build succeeds and generated `EngineRpcService.WorkerStatusPB` exposes `getAvailableKvCache()`, `getTotalKvCache()`, and `getBlockSize()`.

- [ ] **Step 3: Commit**

```bash
git add ../cpp/model_rpc/proto/model_rpc_service.proto
git commit -m "feat: add kv capacity fields to worker status proto"
```

### Task 2: Convert WorkerStatusPB KV Capacity

**Files:**
- Modify: `flexlb-sync/src/main/java/org/flexlb/service/grpc/EngineStatusConverter.java`
- Create: `flexlb-sync/src/test/java/org/flexlb/service/grpc/EngineStatusConverterTest.java`

- [ ] **Step 1: Write the failing converter test**

Create `flexlb-sync/src/test/java/org/flexlb/service/grpc/EngineStatusConverterTest.java`:

```java
package org.flexlb.service.grpc;

import org.flexlb.domain.worker.WorkerStatusResponse;
import org.flexlb.engine.grpc.EngineRpcService;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class EngineStatusConverterTest {

    @Test
    void should_convertKvCapacityFromWorkerStatus() {
        EngineRpcService.WorkerStatusPB workerStatusPB = EngineRpcService.WorkerStatusPB.newBuilder()
                .setRole("PREFILL")
                .setStatusVersion(7L)
                .setAvailableKvCache(100L)
                .setTotalKvCache(300L)
                .setBlockSize(16L)
                .setAlive(true)
                .build();

        WorkerStatusResponse response = EngineStatusConverter.convertToWorkerStatusResponse(workerStatusPB);

        assertNotNull(response.getCacheStatus());
        assertEquals(100L, response.getCacheStatus().getAvailableKvCache());
        assertEquals(300L, response.getCacheStatus().getTotalKvCache());
        assertEquals(16L, response.getCacheStatus().getBlockSize());
        assertEquals(7L, response.getCacheStatus().getVersion());
    }
}
```

- [ ] **Step 2: Run the test and verify it fails**

Run:

```bash
./mvnw -pl flexlb-sync -am -Dtest=EngineStatusConverterTest test
```

Expected: FAIL because `response.getCacheStatus()` is `null`.

- [ ] **Step 3: Implement conversion**

In `EngineStatusConverter.convertToWorkerStatusResponse(...)`, after setting `alive`, add:

```java
        CacheStatus cacheStatus = new CacheStatus();
        cacheStatus.setAvailableKvCache(workerStatusPB.getAvailableKvCache());
        cacheStatus.setTotalKvCache(workerStatusPB.getTotalKvCache());
        cacheStatus.setBlockSize(workerStatusPB.getBlockSize());
        cacheStatus.setVersion(workerStatusPB.getStatusVersion());
        response.setCacheStatus(cacheStatus);
```

- [ ] **Step 4: Run the test and verify it passes**

Run:

```bash
./mvnw -pl flexlb-sync -am -Dtest=EngineStatusConverterTest test
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add flexlb-sync/src/main/java/org/flexlb/service/grpc/EngineStatusConverter.java \
  flexlb-sync/src/test/java/org/flexlb/service/grpc/EngineStatusConverterTest.java
git commit -m "feat: convert worker status kv capacity"
```

### Task 3: Apply WorkerStatus KV Capacity Updates

**Files:**
- Modify: `flexlb-sync/src/main/java/org/flexlb/sync/runner/GrpcWorkerStatusRunner.java`
- Modify: `flexlb-sync/src/test/java/org/flexlb/sync/runner/GrpcWorkerStatusCheckRunnerTest.java`

- [ ] **Step 1: Extend the failing runner test**

In `GrpcWorkerStatusCheckRunnerTest`, add assertions to `should_callGrpcServiceAndVerifyInteraction_when_runnerExecutes()` and set KV fields in the mock response:

```java
        EngineRpcService.WorkerStatusPB workerStatusPB = EngineRpcService.WorkerStatusPB.newBuilder()
                .setRole("test-role")
                .setAvailableConcurrency(10)
                .setRunningQueryLen(5)
                .setWaitingQueryLen(3)
                .setStepLatencyMs(100)
                .setIterateCount(20)
                .setDpSize(2)
                .setTpSize(4)
                .setStatusVersion(100)
                .setAvailableKvCache(1000)
                .setTotalKvCache(3000)
                .setBlockSize(16)
                .setAlive(true)
                .build();
```

Add after the existing `verify(...)`:

```java
        org.junit.jupiter.api.Assertions.assertNotNull(workerStatus.getCacheStatus());
        org.junit.jupiter.api.Assertions.assertEquals(1000L, workerStatus.getCacheStatus().getAvailableKvCache());
        org.junit.jupiter.api.Assertions.assertEquals(3000L, workerStatus.getCacheStatus().getTotalKvCache());
        org.junit.jupiter.api.Assertions.assertEquals(16L, workerStatus.getCacheStatus().getBlockSize());
        org.junit.jupiter.api.Assertions.assertEquals(1000L, workerStatus.getAvailableKvCacheTokens().get());
        org.junit.jupiter.api.Assertions.assertEquals(2000L, workerStatus.getUsedKvCacheTokens().get());
```

- [ ] **Step 2: Run the test and verify it fails**

Run:

```bash
./mvnw -pl flexlb-sync -am -Dtest=GrpcWorkerStatusCheckRunnerTest test
```

Expected: FAIL because worker cache status and token counters are not updated from worker status.

- [ ] **Step 3: Implement KV update in worker status runner**

In `GrpcWorkerStatusRunner`, add import:

```java
import org.flexlb.dao.master.CacheStatus;
```

Add this helper method near `logWorkerStatusUpdate(...)`:

```java
    private void updateKvCacheFromWorkerStatus(WorkerStatusResponse newWorkerStatus) {
        CacheStatus cacheStatus = newWorkerStatus.getCacheStatus();
        if (cacheStatus == null || cacheStatus.getTotalKvCache() <= 0 || cacheStatus.getBlockSize() <= 0) {
            return;
        }
        long latestAvailableKvCacheTokens = cacheStatus.getAvailableKvCache();
        long latestUsedKvCacheTokens = cacheStatus.getTotalKvCache() - latestAvailableKvCacheTokens;
        workerStatus.updateKvCacheTokens(latestUsedKvCacheTokens, latestAvailableKvCacheTokens);
        workerStatus.setCacheStatus(cacheStatus);
    }
```

Call `updateKvCacheFromWorkerStatus(newWorkerStatus);` in both successful response paths after basic worker fields are updated:

```java
                workerStatus.setAlive(newWorkerStatus.isAlive());
                workerStatus.setDpSize(newWorkerStatus.getDpSize());
                workerStatus.setTpSize(newWorkerStatus.getTpSize());
                updateKvCacheFromWorkerStatus(newWorkerStatus);
```

and:

```java
            workerStatus.setAlive(newWorkerStatus.isAlive());
            workerStatus.getStatusVersion().set(responseVersion != null ? responseVersion : -1L);
            workerStatus.getLatestFinishedTaskVersion().set(newWorkerStatus.getLatestFinishedVersion() != null ? newWorkerStatus.getLatestFinishedVersion() : -1L);
            updateKvCacheFromWorkerStatus(newWorkerStatus);
```

- [ ] **Step 4: Run the test and verify it passes**

Run:

```bash
./mvnw -pl flexlb-sync -am -Dtest=GrpcWorkerStatusCheckRunnerTest test
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add flexlb-sync/src/main/java/org/flexlb/sync/runner/GrpcWorkerStatusRunner.java \
  flexlb-sync/src/test/java/org/flexlb/sync/runner/GrpcWorkerStatusCheckRunnerTest.java
git commit -m "feat: update kv capacity from worker status"
```

### Task 4: Add KVCM CacheAwareService Bean

**Files:**
- Modify: `flexlb-cache/src/main/java/org/flexlb/cache/service/impl/DefaultCacheAwareService.java`
- Create: `flexlb-cache/src/main/java/org/flexlb/cache/service/impl/KvcmCacheAwareService.java`
- Create: `flexlb-cache/src/test/java/org/flexlb/cache/service/impl/KvcmCacheAwareServiceTest.java`

- [ ] **Step 1: Write the no-op KVCM service test**

Create `KvcmCacheAwareServiceTest.java`:

```java
package org.flexlb.cache.service.impl;

import org.flexlb.cache.domain.WorkerCacheUpdateResult;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.route.RoleType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KvcmCacheAwareServiceTest {

    @Test
    void should_returnEmptyMatchesInPhaseOne() {
        KvcmCacheAwareService service = new KvcmCacheAwareService();

        Map<String, Integer> matches = service.findMatchingEngines(List.of(1L, 2L), RoleType.PREFILL, "group-a");

        assertTrue(matches.isEmpty());
    }

    @Test
    void should_noopUpdateEngineBlockCacheSuccessfully() {
        KvcmCacheAwareService service = new KvcmCacheAwareService();
        WorkerStatus workerStatus = new WorkerStatus();
        workerStatus.setIp("127.0.0.1");
        workerStatus.setPort(8080);

        WorkerCacheUpdateResult result = service.updateEngineBlockCache(workerStatus);

        assertTrue(result.isSuccess());
        assertEquals("127.0.0.1:8080", result.getEngineIpPort());
        assertEquals(0L, result.getCacheBlockCount());
    }
}
```

- [ ] **Step 2: Run the test and verify it fails**

Run:

```bash
./mvnw -pl flexlb-cache -am -Dtest=KvcmCacheAwareServiceTest test
```

Expected: FAIL because `KvcmCacheAwareService` does not exist.

- [ ] **Step 3: Add conditional annotations**

In `DefaultCacheAwareService`, add import:

```java
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
```

Add annotation above the class:

```java
@ConditionalOnProperty(prefix = "kvcm", name = "enable", havingValue = "false", matchIfMissing = true)
```

- [ ] **Step 4: Implement `KvcmCacheAwareService`**

Create `flexlb-cache/src/main/java/org/flexlb/cache/service/impl/KvcmCacheAwareService.java`:

```java
package org.flexlb.cache.service.impl;

import lombok.extern.slf4j.Slf4j;
import org.flexlb.cache.domain.WorkerCacheUpdateResult;
import org.flexlb.cache.service.CacheAwareService;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.route.RoleType;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@ConditionalOnProperty(prefix = "kvcm", name = "enable", havingValue = "true")
public class KvcmCacheAwareService implements CacheAwareService {

    @Override
    public Map<String, Integer> findMatchingEngines(List<Long> blockCacheKeys, RoleType roleType, String group) {
        log.debug("KVCM cache lookup is not implemented in phase one, roleType={}, group={}", roleType, group);
        return Collections.emptyMap();
    }

    @Override
    public WorkerCacheUpdateResult updateEngineBlockCache(WorkerStatus workerStatus) {
        String engineIpPort = workerStatus != null ? workerStatus.getIpPort() : null;
        return WorkerCacheUpdateResult.builder()
                .success(true)
                .engineIpPort(engineIpPort)
                .cacheBlockCount(0)
                .build();
    }
}
```

- [ ] **Step 5: Run the test and verify it passes**

Run:

```bash
./mvnw -pl flexlb-cache -am -Dtest=KvcmCacheAwareServiceTest test
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add flexlb-cache/src/main/java/org/flexlb/cache/service/impl/DefaultCacheAwareService.java \
  flexlb-cache/src/main/java/org/flexlb/cache/service/impl/KvcmCacheAwareService.java \
  flexlb-cache/src/test/java/org/flexlb/cache/service/impl/KvcmCacheAwareServiceTest.java
git commit -m "feat: add kvcm cache aware service"
```

### Task 5: Skip GetCacheStatus in KVCM Mode

**Files:**
- Modify: `flexlb-sync/src/main/java/org/flexlb/sync/runner/EngineSyncRunner.java`
- Modify: `flexlb-sync/src/main/java/org/flexlb/sync/synchronizer/MasterEngineSynchronizer.java`
- Modify: `flexlb-sync/src/test/java/org/flexlb/sync/runner/EngineSyncRunnerTest.java`

- [ ] **Step 1: Write the failing runner test**

In `EngineSyncRunnerTest`, add imports:

```java
import org.flexlb.dao.master.WorkerHost;
import org.junit.jupiter.api.Assertions;

import java.util.List;
```

Update existing constructor calls to pass `false` as the final argument.

Add this test:

```java
    @Test
    void should_skipCacheStatusRunner_when_kvcmEnabled() {
        WorkerHost workerHost = new WorkerHost();
        workerHost.setIp("127.0.0.1");
        workerHost.setPort(8080);
        workerHost.setSite("test-site");
        workerHost.setGroup("test-group");

        org.mockito.Mockito.when(roleType.toString()).thenReturn("PREFILL");
        org.mockito.Mockito.when(workerAddressService.getEngineWorkerList(modelName, roleType))
                .thenReturn(List.of(workerHost));

        EngineSyncRunner runner = new EngineSyncRunner(
                modelName,
                workerStatusMap,
                workerAddressService,
                statusCheckExecutor,
                engineHealthReporter,
                engineGrpcService,
                roleType,
                localKvCacheAwareManager,
                syncRequestTimeoutMs,
                syncCount,
                syncEngineStatusInterval,
                true
        );

        runner.run();

        org.mockito.Mockito.verify(statusCheckExecutor, org.mockito.Mockito.times(1))
                .submit(org.mockito.ArgumentMatchers.any(Runnable.class));
        WorkerStatus workerStatus = workerStatusMap.get("127.0.0.1:8080");
        Assertions.assertNotNull(workerStatus);
        Assertions.assertFalse(workerStatus.getCacheCheckInProgress().get());
    }
```

- [ ] **Step 2: Run the test and verify it fails**

Run:

```bash
./mvnw -pl flexlb-sync -am -Dtest=EngineSyncRunnerTest test
```

Expected: FAIL because `EngineSyncRunner` has no KVCM constructor argument and still submits cache checks.

- [ ] **Step 3: Add `kvcmEnabled` to `EngineSyncRunner`**

Add field:

```java
    private final boolean kvcmEnabled;
```

Update constructor signature:

```java
                            LongAdder syncCount,
                            Long syncEngineStatusInterval,
                            boolean kvcmEnabled) {
```

Set the field:

```java
        this.kvcmEnabled = kvcmEnabled;
```

Wrap cache runner submission:

```java
                if (kvcmEnabled) {
                    workerStatus.getCacheCheckInProgress().set(false);
                    logger.debug("Skip cache status check for worker: {} because KVCM is enabled", workerIpPort);
                } else if (workerStatus.getCacheCheckInProgress().compareAndSet(false, true)) {
                    logger.debug("Submitting GrpcCacheStatusCheckRunner for worker: {}, site: {}", workerIpPort, site);
                    GrpcCacheStatusCheckRunner grpcCacheStatusCheckRunner
                            = new GrpcCacheStatusCheckRunner(modelName, workerIpPort, site, roleType,
                            workerStatus, engineHealthReporter, engineGrpcService, localKvCacheAwareManager,
                            syncRequestTimeoutMs, syncCount, syncEngineStatusInterval);
                    statusCheckExecutor.submit(grpcCacheStatusCheckRunner);
                } else {
                    logger.info("Skip cache check for worker: {}, previous request in progress", workerIpPort);
                }
```

- [ ] **Step 4: Wire Spring property in `MasterEngineSynchronizer`**

Add import:

```java
import org.springframework.beans.factory.annotation.Value;
```

Add field:

```java
    private final boolean kvcmEnabled;
```

Update constructor parameters:

```java
                                    CacheAwareService localKvCacheAwareManager,
                                    @Value("${kvcm.enable:false}") boolean kvcmEnabled) {
```

Set the field:

```java
        this.kvcmEnabled = kvcmEnabled;
```

Pass the flag into `EngineSyncRunner`:

```java
                                syncRequestTimeoutMs, syncCount, syncEngineStatusInterval, kvcmEnabled
```

- [ ] **Step 5: Run the test and verify it passes**

Run:

```bash
./mvnw -pl flexlb-sync -am -Dtest=EngineSyncRunnerTest test
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add flexlb-sync/src/main/java/org/flexlb/sync/runner/EngineSyncRunner.java \
  flexlb-sync/src/main/java/org/flexlb/sync/synchronizer/MasterEngineSynchronizer.java \
  flexlb-sync/src/test/java/org/flexlb/sync/runner/EngineSyncRunnerTest.java
git commit -m "feat: skip cache status polling in kvcm mode"
```

### Task 6: Final Verification

**Files:**
- No code changes expected.

- [ ] **Step 1: Run targeted tests**

Run:

```bash
./mvnw -pl flexlb-cache,flexlb-sync -am \
  -Dtest=KvcmCacheAwareServiceTest,EngineStatusConverterTest,GrpcWorkerStatusCheckRunnerTest,EngineSyncRunnerTest \
  test
```

Expected: PASS.

- [ ] **Step 2: Run compile across affected modules**

Run:

```bash
./mvnw -pl flexlb-grpc,flexlb-cache,flexlb-sync -am test -DskipTests
```

Expected: PASS.

- [ ] **Step 3: Review diff**

Run:

```bash
git diff --stat HEAD~5..HEAD
git diff HEAD~5..HEAD -- ../cpp/model_rpc/proto/model_rpc_service.proto flexlb-cache flexlb-sync
```

Expected: diff only includes proto extension, KVCM no-op service, KV capacity conversion/update, KVCM cache polling switch, and tests.

- [ ] **Step 4: Commit any verification-only fixes**

If verification required small fixes, commit them:

```bash
git add <fixed-files>
git commit -m "fix: align kvcm compatibility tests"
```

Expected: no commit if Step 1 and Step 2 passed without changes.

## Self-Review

- Spec coverage: proto compatibility is Task 1; worker status conversion is Task 2; worker status KV application is Task 3; KVCM bean prewire is Task 4; `GetCacheStatus` skipping is Task 5; validation is Task 6.
- Placeholder scan: no `TBD` or deferred implementation steps are used. Phase-two KVCM lookup is explicitly a non-goal and represented by a no-op implementation.
- Type consistency: the plan uses `KVCM_ENABLE=true` as the environment variable, `kvcm.enable=true` as the Spring property, and `@ConditionalOnProperty(prefix = "kvcm", name = "enable")` for bean selection.
