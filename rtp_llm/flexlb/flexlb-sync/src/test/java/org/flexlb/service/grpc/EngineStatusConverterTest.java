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
