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
