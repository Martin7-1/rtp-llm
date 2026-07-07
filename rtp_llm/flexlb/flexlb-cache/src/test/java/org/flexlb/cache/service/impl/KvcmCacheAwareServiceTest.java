package org.flexlb.cache.service.impl;

import org.flexlb.cache.domain.WorkerCacheUpdateResult;
import org.flexlb.cache.service.CacheAwareService;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.route.RoleType;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.MapPropertySource;

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

    @Test
    void should_useDefaultServiceWhenKvcmEnableIsMissing() {
        assertCacheAwareServiceBean(Map.of(), DefaultCacheAwareService.class);
    }

    @Test
    void should_useDefaultServiceWhenKvcmEnableIsFalse() {
        assertCacheAwareServiceBean(Map.of("kvcm.enable", "false"), DefaultCacheAwareService.class);
    }

    @Test
    void should_useKvcmServiceWhenKvcmEnableIsTrue() {
        assertCacheAwareServiceBean(Map.of("kvcm.enable", "true"), KvcmCacheAwareService.class);
    }

    private void assertCacheAwareServiceBean(Map<String, Object> properties,
        Class<? extends CacheAwareService> expectedServiceType) {

        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            context.getEnvironment().getPropertySources()
                .addFirst(new MapPropertySource("testProperties", properties));
            context.register(DefaultCacheAwareService.class, KvcmCacheAwareService.class);
            if (context.getBeanFactory().containsBeanDefinition("defaultCacheAwareService")) {
                context.getBeanFactory().getBeanDefinition("defaultCacheAwareService").setLazyInit(true);
            }
            if (context.getBeanFactory().containsBeanDefinition("kvcmCacheAwareService")) {
                context.getBeanFactory().getBeanDefinition("kvcmCacheAwareService").setLazyInit(true);
            }
            context.refresh();

            String[] serviceNames = context.getBeanNamesForType(CacheAwareService.class, false, false);

            assertEquals(1, serviceNames.length);
            assertEquals(expectedServiceType, context.getType(serviceNames[0]));
        }
    }
}
