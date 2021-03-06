package com.sequenceiq.cloudbreak.orchestrator;

import java.util.Map;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.sequenceiq.cloudbreak.orchestrator.containers.ContainerBootstrap;

public class SimpleContainerBootstrapRunner implements Callable<Boolean> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleContainerBootstrapRunner.class);
    private static final int MAX_RETRY_COUNT = 7;
    private static final int SLEEP_TIME = 10000;

    private final ContainerBootstrap containerBootstrap;
    private final Map<String, String> mdcMap;

    private SimpleContainerBootstrapRunner(ContainerBootstrap containerBootstrap, Map<String, String> mdcReplica) {
        this.containerBootstrap = containerBootstrap;
        this.mdcMap = mdcReplica;
    }

    @Override
    public Boolean call() throws Exception {
        MDC.setContextMap(mdcMap);
        boolean success = false;
        int retryCount = 0;
        Exception actualException = null;
        while (!success && MAX_RETRY_COUNT >= retryCount) {
            try {
                containerBootstrap.call();
                success = true;
                LOGGER.info("Container started successfully.");
            } catch (Exception ex) {
                success = false;
                actualException = ex;
                retryCount++;
                LOGGER.error(String.format("Container failed to start, retrying [%s/%s]: %s", MAX_RETRY_COUNT, retryCount, ex.getMessage()));
                Thread.sleep(SLEEP_TIME);
            }
        }
        if (!success) {
            LOGGER.error(String.format("Container failed to start in %s attempts: %s", MAX_RETRY_COUNT, actualException));
            throw actualException;
        }
        return Boolean.TRUE;
    }

    public static SimpleContainerBootstrapRunner simpleContainerBootstrapRunner(ContainerBootstrap containerBootstrap, Map<String, String> mdcMap) {
        return new SimpleContainerBootstrapRunner(containerBootstrap, mdcMap);
    }
}
