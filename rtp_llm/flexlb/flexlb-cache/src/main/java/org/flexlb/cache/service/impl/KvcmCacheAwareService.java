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
