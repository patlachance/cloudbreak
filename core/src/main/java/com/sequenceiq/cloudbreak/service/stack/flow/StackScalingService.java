package com.sequenceiq.cloudbreak.service.stack.flow;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.ecwid.consul.v1.ConsulClient;
import com.sequenceiq.cloudbreak.core.flow.service.AmbariHostsRemover;
import com.sequenceiq.cloudbreak.domain.BillingStatus;
import com.sequenceiq.cloudbreak.domain.CloudPlatform;
import com.sequenceiq.cloudbreak.domain.InstanceGroup;
import com.sequenceiq.cloudbreak.domain.InstanceGroupType;
import com.sequenceiq.cloudbreak.domain.InstanceMetaData;
import com.sequenceiq.cloudbreak.domain.InstanceStatus;
import com.sequenceiq.cloudbreak.domain.Resource;
import com.sequenceiq.cloudbreak.domain.Stack;
import com.sequenceiq.cloudbreak.repository.RetryingStackUpdater;
import com.sequenceiq.cloudbreak.service.PollingService;
import com.sequenceiq.cloudbreak.service.events.CloudbreakEventService;
import com.sequenceiq.cloudbreak.service.stack.StackService;
import com.sequenceiq.cloudbreak.service.stack.connector.CloudPlatformConnector;
import com.sequenceiq.cloudbreak.service.stack.connector.UserDataBuilder;

@Service
public class StackScalingService {
    private static final Logger LOGGER = LoggerFactory.getLogger(StackScalingService.class);
    private static final int POLLING_INTERVAL = 5000;
    private static final int MAX_POLLING_ATTEMPTS = 100;

    @Autowired
    private StackService stackService;

    @Autowired
    private RetryingStackUpdater stackUpdater;

    @Autowired
    private UserDataBuilder userDataBuilder;

    @Autowired
    private PollingService<ConsulContext> consulPollingService;

    @Autowired
    private ConsulAgentLeaveCheckerTask consulAgentLeaveCheckerTask;

    @Autowired
    private CloudbreakEventService eventService;

    @Autowired
    private AmbariHostsRemover ambariHostsRemover;

    @javax.annotation.Resource
    private Map<CloudPlatform, CloudPlatformConnector> cloudPlatformConnectors;

    public Set<Resource> addInstances(Long stackId, String instanceGroupName, Integer scalingAdjustment) throws Exception {
        Set<Resource> resources;
        Stack stack = stackService.getById(stackId);
        Map<InstanceGroupType, String> userData = userDataBuilder.buildUserData(stack.cloudPlatform());
        resources = cloudPlatformConnectors.get(stack.cloudPlatform())
                .addInstances(stack, userData.get(InstanceGroupType.GATEWAY), userData.get(InstanceGroupType.CORE), scalingAdjustment, instanceGroupName);
        return resources;
    }

    public void downscaleStack(Long stackId, String instanceGroupName, Integer scalingAdjustment) throws Exception {
        Stack stack = stackService.getById(stackId);
        Map<String, String> unregisteredHostNamesByInstanceId = getUnregisteredInstanceIds(instanceGroupName, scalingAdjustment, stack);
        Set<String> instanceIds = new HashSet<>(unregisteredHostNamesByInstanceId.keySet());
        deleteHostsFromAmbari(stack, unregisteredHostNamesByInstanceId);
        instanceIds = cloudPlatformConnectors.get(stack.cloudPlatform()).removeInstances(stack, instanceIds, instanceGroupName);
        updateRemovedResourcesState(stack, instanceIds, stack.getInstanceGroupByInstanceGroupName(instanceGroupName));
    }

    private Map<String, String> getUnregisteredInstanceIds(String instanceGroupName, Integer scalingAdjustment, Stack stack) {
        Map<String, String> instanceIds = new HashMap<>();

        int i = 0;
        for (InstanceMetaData metaData : stack.getInstanceGroupByInstanceGroupName(instanceGroupName).getInstanceMetaData()) {
            if (!metaData.getAmbariServer() && !metaData.getConsulServer() && (metaData.isDecommissioned() || metaData.isUnRegistered())) {
                instanceIds.put(metaData.getInstanceId(), metaData.getDiscoveryFQDN());
                if (++i >= scalingAdjustment * -1) {
                    break;
                }
            }
        }
        return instanceIds;
    }

    private void deleteHostsFromAmbari(Stack stack, Map<String, String> unregisteredHostNamesByInstanceId) {
        if (stack.getCluster() == null) {
            List<String> hostList = new ArrayList<>(unregisteredHostNamesByInstanceId.values());
            ambariHostsRemover.deleteHosts(stack, hostList, new ArrayList<String>());
        }
    }

    private void updateRemovedResourcesState(Stack stack, Set<String> instanceIds, InstanceGroup instanceGroup) {
        int nodeCount = instanceGroup.getNodeCount() - instanceIds.size();
        stackUpdater.updateNodeCount(stack.getId(), nodeCount, instanceGroup.getGroupName());

        List<ConsulClient> clients = createConsulClients(stack, instanceGroup.getGroupName());
        for (InstanceMetaData instanceMetaData : instanceGroup.getInstanceMetaData()) {
            if (instanceIds.contains(instanceMetaData.getInstanceId())) {
                long timeInMillis = Calendar.getInstance().getTimeInMillis();
                instanceMetaData.setTerminationDate(timeInMillis);
                instanceMetaData.setInstanceStatus(InstanceStatus.TERMINATED);
                removeAgentFromConsul(stack, clients, instanceMetaData);
            }
        }

        stackUpdater.updateStackMetaData(stack.getId(), instanceGroup.getAllInstanceMetaData(), instanceGroup.getGroupName());
        LOGGER.info("Successfully terminated metadata of instances '{}' in stack.", instanceIds);
        eventService.fireCloudbreakEvent(stack.getId(), BillingStatus.BILLING_CHANGED.name(),
                "Billing changed due to downscaling of cluster infrastructure.");
    }

    private List<ConsulClient> createConsulClients(Stack stack, String instanceGroupName) {
        return ConsulUtils.createClients(stack.getGatewayInstanceGroup().getInstanceMetaData());
    }

    private void removeAgentFromConsul(Stack stack, List<ConsulClient> clients, InstanceMetaData metaData) {
        String nodeName = metaData.getDiscoveryFQDN().replace(ConsulUtils.CONSUL_DOMAIN, "");
        consulPollingService.pollWithTimeout(
                consulAgentLeaveCheckerTask,
                new ConsulContext(stack, clients, Collections.singletonList(nodeName)),
                POLLING_INTERVAL,
                MAX_POLLING_ATTEMPTS);
    }
}
