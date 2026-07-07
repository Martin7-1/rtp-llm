package org.flexlb.sync.runner;

import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.route.RoleType;
import org.flexlb.engine.grpc.EngineRpcService;
import org.flexlb.service.grpc.EngineGrpcService;
import org.flexlb.service.monitor.EngineHealthReporter;
import org.flexlb.metric.NoOpFlexMonitor;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class GrpcWorkerStatusCheckRunnerTest {

    private final EngineHealthReporter engineHealthReporter = noOpEngineHealthReporter();

    @Test
    void should_callGrpcServiceAndVerifyInteraction_when_runnerExecutes() {
        // Arrange
        String modelName = "test-model";
        String ipPort = "127.0.0.1:8080";
        String site = "test-site";
        String group = "test-group";

        WorkerStatus workerStatus = new WorkerStatus();
        workerStatus.setIp("127.0.0.1");
        workerStatus.setPort(8080);

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

        RecordingEngineGrpcService engineGrpcService = new RecordingEngineGrpcService(workerStatusPB);

        // Act
        GrpcWorkerStatusRunner runner = new GrpcWorkerStatusRunner(
                modelName, ipPort, site,
                RoleType.PREFILL,
                group, workerStatus, engineHealthReporter, engineGrpcService, 20);
        runner.run();

        // Assert
        assertEquals("127.0.0.1", engineGrpcService.ip);
        assertEquals(8081, engineGrpcService.grpcPort);
        assertEquals(-1L, engineGrpcService.finishedTaskVersion);
        assertEquals(20L, engineGrpcService.requestTimeoutMs);
        assertEquals(RoleType.PREFILL, engineGrpcService.roleType);
        assertNotNull(workerStatus.getCacheStatus());
        assertEquals(1000L, workerStatus.getCacheStatus().getAvailableKvCache());
        assertEquals(3000L, workerStatus.getCacheStatus().getTotalKvCache());
        assertEquals(16L, workerStatus.getCacheStatus().getBlockSize());
        assertEquals(1000L, workerStatus.getAvailableKvCacheTokens().get());
        assertEquals(2000L, workerStatus.getUsedKvCacheTokens().get());
    }

    @Test
    void should_ignoreDefaultKvCapacity_when_runnerExecutesWithOldWorkerStatus() {
        // Arrange
        String modelName = "test-model";
        String ipPort = "127.0.0.1:8080";
        String site = "test-site";
        String group = "test-group";

        WorkerStatus workerStatus = new WorkerStatus();
        workerStatus.setIp("127.0.0.1");
        workerStatus.setPort(8080);

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
                .setAlive(true)
                .build();

        RecordingEngineGrpcService engineGrpcService = new RecordingEngineGrpcService(workerStatusPB);

        // Act
        GrpcWorkerStatusRunner runner = new GrpcWorkerStatusRunner(
                modelName, ipPort, site,
                RoleType.PREFILL,
                group, workerStatus, engineHealthReporter, engineGrpcService, 20);
        runner.run();

        // Assert
        assertEquals("127.0.0.1", engineGrpcService.ip);
        assertEquals(8081, engineGrpcService.grpcPort);
        assertEquals(-1L, engineGrpcService.finishedTaskVersion);
        assertEquals(20L, engineGrpcService.requestTimeoutMs);
        assertEquals(RoleType.PREFILL, engineGrpcService.roleType);
        assertNull(workerStatus.getCacheStatus());
        assertEquals(0L, workerStatus.getAvailableKvCacheTokens().get());
        assertEquals(0L, workerStatus.getUsedKvCacheTokens().get());
    }

    private static EngineHealthReporter noOpEngineHealthReporter() {
        try {
            Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
            Field unsafeField = unsafeClass.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            Object unsafe = unsafeField.get(null);
            Method allocateInstance = unsafeClass.getMethod("allocateInstance", Class.class);
            EngineHealthReporter reporter = (EngineHealthReporter) allocateInstance.invoke(unsafe, EngineHealthReporter.class);

            Field monitorField = EngineHealthReporter.class.getDeclaredField("monitor");
            monitorField.setAccessible(true);
            monitorField.set(reporter, NoOpFlexMonitor.getInstance());
            return reporter;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to create no-op EngineHealthReporter", e);
        }
    }

    private static class RecordingEngineGrpcService extends EngineGrpcService {
        private final EngineRpcService.WorkerStatusPB workerStatusPB;
        private String ip;
        private int grpcPort;
        private long finishedTaskVersion;
        private long requestTimeoutMs;
        private RoleType roleType;

        private RecordingEngineGrpcService(EngineRpcService.WorkerStatusPB workerStatusPB) {
            super(null);
            this.workerStatusPB = workerStatusPB;
        }

        @Override
        public EngineRpcService.WorkerStatusPB getWorkerStatus(String ip, int grpcPort, long finishedTaskVersion,
                                                               long requestTimeoutMs, RoleType roleType) {
            this.ip = ip;
            this.grpcPort = grpcPort;
            this.finishedTaskVersion = finishedTaskVersion;
            this.requestTimeoutMs = requestTimeoutMs;
            this.roleType = roleType;
            return workerStatusPB;
        }
    }
}
