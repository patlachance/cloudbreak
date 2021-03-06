package com.sequenceiq.cloudbreak.orchestrator;

import java.util.HashSet;
import java.util.Set;

public class ContainerOrchestratorCluster {
    private String apiAddress;
    private Set<Node> nodes = new HashSet<>();

    public ContainerOrchestratorCluster(String apiAddress, Set<Node> nodes) {
        this.apiAddress = apiAddress;
        this.nodes = nodes;
    }

    public String getApiAddress() {
        return apiAddress;
    }

    public Set<Node> getNodes() {
        return nodes;
    }
}
