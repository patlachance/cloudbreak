package com.sequenceiq.cloudbreak.core.flow.service;

import static com.sequenceiq.cloudbreak.domain.Status.AVAILABLE;
import static com.sequenceiq.cloudbreak.domain.Status.CREATE_FAILED;
import static com.sequenceiq.cloudbreak.domain.Status.ENABLE_SECURITY_FAILED;
import static com.sequenceiq.cloudbreak.domain.Status.START_FAILED;
import static com.sequenceiq.cloudbreak.domain.Status.START_IN_PROGRESS;
import static com.sequenceiq.cloudbreak.domain.Status.START_REQUESTED;
import static com.sequenceiq.cloudbreak.domain.Status.STOPPED;
import static com.sequenceiq.cloudbreak.domain.Status.STOP_FAILED;
import static com.sequenceiq.cloudbreak.domain.Status.STOP_IN_PROGRESS;
import static com.sequenceiq.cloudbreak.domain.Status.UPDATE_FAILED;
import static com.sequenceiq.cloudbreak.domain.Status.UPDATE_IN_PROGRESS;
import static com.sequenceiq.cloudbreak.service.PollingResult.isSuccess;

import java.util.Date;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.sequenceiq.ambari.client.AmbariClient;
import com.sequenceiq.cloudbreak.core.CloudbreakException;
import com.sequenceiq.cloudbreak.core.bootstrap.service.ClusterContainerRunner;
import com.sequenceiq.cloudbreak.core.flow.context.ClusterScalingContext;
import com.sequenceiq.cloudbreak.core.flow.context.FlowContext;
import com.sequenceiq.cloudbreak.core.flow.context.ProvisioningContext;
import com.sequenceiq.cloudbreak.core.flow.context.StackScalingContext;
import com.sequenceiq.cloudbreak.core.flow.context.StackStatusUpdateContext;
import com.sequenceiq.cloudbreak.domain.Cluster;
import com.sequenceiq.cloudbreak.domain.HostGroup;
import com.sequenceiq.cloudbreak.domain.InstanceGroup;
import com.sequenceiq.cloudbreak.domain.InstanceMetaData;
import com.sequenceiq.cloudbreak.domain.InstanceStatus;
import com.sequenceiq.cloudbreak.domain.Stack;
import com.sequenceiq.cloudbreak.domain.Status;
import com.sequenceiq.cloudbreak.logger.MDCBuilder;
import com.sequenceiq.cloudbreak.repository.RetryingStackUpdater;
import com.sequenceiq.cloudbreak.service.PollingResult;
import com.sequenceiq.cloudbreak.service.PollingService;
import com.sequenceiq.cloudbreak.service.cluster.AmbariClientProvider;
import com.sequenceiq.cloudbreak.service.cluster.ClusterService;
import com.sequenceiq.cloudbreak.service.cluster.flow.AmbariClusterConnector;
import com.sequenceiq.cloudbreak.service.cluster.flow.ClusterSecurityService;
import com.sequenceiq.cloudbreak.service.cluster.flow.EmailSenderService;
import com.sequenceiq.cloudbreak.service.events.CloudbreakEventService;
import com.sequenceiq.cloudbreak.service.hostgroup.HostGroupService;
import com.sequenceiq.cloudbreak.service.stack.StackService;
import com.sequenceiq.cloudbreak.service.stack.flow.AmbariStartupListenerTask;
import com.sequenceiq.cloudbreak.service.stack.flow.AmbariStartupPollerObject;

@Service
public class AmbariClusterFacade implements ClusterFacade {
    private static final Logger LOGGER = LoggerFactory.getLogger(AmbariClusterFacade.class);
    private static final int POLLING_INTERVAL = 5000;
    private static final int MS_PER_SEC = 1000;
    private static final int SEC_PER_MIN = 60;
    private static final int MAX_POLLING_ATTEMPTS = SEC_PER_MIN / (POLLING_INTERVAL / MS_PER_SEC) * 10;
    private static final String ADMIN = "admin";

    @Autowired
    private AmbariClientProvider ambariClientProvider;

    @Autowired
    private AmbariStartupListenerTask ambariStartupListenerTask;

    @Autowired
    private RetryingStackUpdater stackUpdater;

    @Autowired
    private AmbariClusterConnector ambariClusterConnector;

    @Autowired
    private StackService stackService;

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private PollingService<AmbariStartupPollerObject> ambariStartupPollerObjectPollingService;

    @Autowired
    private EmailSenderService emailSenderService;

    @Autowired
    private CloudbreakEventService eventService;

    @Autowired
    private HostGroupService hostGroupService;

    @Autowired
    private ClusterSecurityService securityService;

    @Autowired
    private ClusterContainerRunner containerRunner;

    @Override
    public FlowContext startAmbari(FlowContext context) throws Exception {
        ProvisioningContext provisioningContext = (ProvisioningContext) context;
        Stack stack = stackService.getById(provisioningContext.getStackId());
        MDCBuilder.buildMdcContext(stack);
        Cluster cluster = stack.getCluster();
        if (cluster == null) {
            LOGGER.debug("There is no cluster installed on the stack, skipping start Ambari step");
        } else {
            MDCBuilder.buildMdcContext(cluster);
            LOGGER.debug("Starting Ambari. Context: {}", context);
            AmbariStartupPollerObject ambariStartupPollerObject = new AmbariStartupPollerObject(stack, provisioningContext.getAmbariIp(),
                    ambariClientProvider.getDefaultAmbariClient(provisioningContext.getAmbariIp()));

            PollingResult pollingResult = ambariStartupPollerObjectPollingService.pollWithTimeout(ambariStartupListenerTask, ambariStartupPollerObject,
                    POLLING_INTERVAL, MAX_POLLING_ATTEMPTS);

            if (isSuccess(pollingResult)) {
                LOGGER.info("Ambari has successfully started! Polling result: {}", pollingResult);
                assert provisioningContext.getAmbariIp() != null;

            } else {
                LOGGER.info("Could not start Ambari. polling result: {},  Context: {}", pollingResult, context);
                throw new CloudbreakException(String.format("Could not start Ambari. polling result: '%s',  Context: '%s'", pollingResult, context));
            }
            clusterService.updateAmbariIp(cluster.getId(), provisioningContext.getAmbariIp());
            changeAmbariCredentials(provisioningContext.getAmbariIp(), cluster);
        }
        return provisioningContext;
    }

    @Override
    public FlowContext buildAmbariCluster(FlowContext context) throws Exception {
        ProvisioningContext provisioningContext = (ProvisioningContext) context;
        Stack stack = stackService.getById(provisioningContext.getStackId());
        MDCBuilder.buildMdcContext(stack);
        if (stack.getCluster() == null) {
            LOGGER.debug("There is no cluster installed on the stack, skipping build Ambari step");
        } else {
            MDCBuilder.buildMdcContext(stack.getCluster());
            LOGGER.debug("Building Ambari cluster. Context: {}", context);
            LOGGER.info("Starting Ambari cluster [Ambari server address: {}]", stack.getAmbariIp());
            stackUpdater.updateStackStatus(stack.getId(), UPDATE_IN_PROGRESS, "Building the Ambari cluster.");
            Cluster cluster = ambariClusterConnector.buildAmbariCluster(stack);
            updateClusterStatus(stack.getId(), AVAILABLE, "");
            stackUpdater.updateStackStatus(stack.getId(), AVAILABLE, "Cluster installation successfully finished. AMBARI_IP:" + stack.getAmbariIp());
            if (cluster.getEmailNeeded()) {
                emailSenderService.sendSuccessEmail(cluster.getOwner(), stack.getAmbariIp());
            }
        }
        return provisioningContext;
    }

    @Override
    public FlowContext startCluster(FlowContext context) throws CloudbreakException {
        StackStatusUpdateContext startContext = (StackStatusUpdateContext) context;
        Stack stack = stackService.getById(startContext.getStackId());
        Cluster cluster = stack.getCluster();
        MDCBuilder.buildMdcContext(cluster);
        LOGGER.debug("Starting cluster. Context: {}", context);
        if (cluster != null && cluster.isStartRequested()) {
            clusterService.updateClusterStatusByStackId(stack.getId(), START_IN_PROGRESS, "Services are starting.");
            stackUpdater.updateStackStatus(stack.getId(), UPDATE_IN_PROGRESS, "Services are starting.");
            ambariClusterConnector.startCluster(stack);
            LOGGER.info("Successfully started Hadoop services, setting cluster state to: {}", AVAILABLE);
            cluster.setUpSince(new Date().getTime());
            cluster.setStatus(AVAILABLE);
            clusterService.updateCluster(cluster);
            stackUpdater.updateStackStatus(stack.getId(), AVAILABLE, "Cluster started successfully.");
            LOGGER.debug("Cluster STARTED.");
        } else {
            LOGGER.info("Cluster start has not been requested, start cluster later.");
        }
        return context;
    }

    @Override
    public FlowContext stopCluster(FlowContext context) throws CloudbreakException {
        StackStatusUpdateContext statusUpdateContext = (StackStatusUpdateContext) context;
        stackUpdater.updateStackStatus(statusUpdateContext.getStackId(), UPDATE_IN_PROGRESS);
        updateClusterStatus(statusUpdateContext.getStackId(), STOP_IN_PROGRESS, "Services are stopping.");
        Cluster cluster = clusterService.retrieveClusterByStackId(statusUpdateContext.getStackId());
        Stack stack = stackService.getById(statusUpdateContext.getStackId());
        MDCBuilder.buildMdcContext(cluster);
        LOGGER.debug("Stopping cluster. Context: {}", context);
        ambariClusterConnector.stopCluster(stack);
        cluster.setStatus(STOPPED);
        stackUpdater.updateStackStatus(statusUpdateContext.getStackId(), STOPPED);
        clusterService.updateCluster(cluster);
        eventService.fireCloudbreakEvent(statusUpdateContext.getStackId(), STOPPED.name(), "Services have been stopped successfully.");
        if (stack.isStopRequested()) {
            LOGGER.info("Hadoop services stopped, stack stop requested.");
        }
        return context;
    }

    @Override
    public FlowContext handleStartFailure(FlowContext context) throws CloudbreakException {
        LOGGER.debug("Handling cluster start failure. Context: {} ", context);
        return updateStackAndClusterStatus(context, START_FAILED);
    }

    @Override
    public FlowContext handleStopFailure(FlowContext context) throws CloudbreakException {
        LOGGER.debug("Handling cluster stop failure. Context: {} ", context);
        return updateStackAndClusterStatus(context, STOP_FAILED);
    }

    @Override
    public FlowContext handleClusterCreationFailure(FlowContext flowContext) throws CloudbreakException {
        ProvisioningContext context = (ProvisioningContext) flowContext;
        MDCBuilder.buildMdcContext(clusterService.retrieveClusterByStackId(context.getStackId()));
        LOGGER.debug("Handling cluster creation failure. Context: {}", flowContext);
        Cluster cluster = clusterService.updateClusterStatusByStackId(context.getStackId(), CREATE_FAILED, context.getErrorReason());
        stackUpdater.updateStackStatus(context.getStackId(), AVAILABLE, "Cluster installation failed. Error: " + context.getErrorReason());
        eventService.fireCloudbreakEvent(context.getStackId(), CREATE_FAILED.name(), context.getErrorReason());
        if (cluster.getEmailNeeded()) {
            emailSenderService.sendFailureEmail(cluster.getOwner());
        }
        return context;
    }

    @Override
    public FlowContext handleSecurityEnableFailure(FlowContext flowContext) throws CloudbreakException {
        ProvisioningContext context = (ProvisioningContext) flowContext;
        Cluster cluster = clusterService.updateClusterStatusByStackId(context.getStackId(), ENABLE_SECURITY_FAILED, context.getErrorReason());
        MDCBuilder.buildMdcContext(cluster);
        eventService.fireCloudbreakEvent(context.getStackId(), ENABLE_SECURITY_FAILED.name(), context.getErrorReason());
        if (cluster.getEmailNeeded()) {
            emailSenderService.sendFailureEmail(cluster.getOwner());
        }
        return context;
    }

    @Override
    public FlowContext addClusterContainers(FlowContext context) throws CloudbreakException {
        try {
            ClusterScalingContext clusterScalingContext = (ClusterScalingContext) context;
            stackUpdater.updateStackStatus(clusterScalingContext.getStackId(), UPDATE_IN_PROGRESS, "Adding new host(s) to the cluster.");
            containerRunner.addClusterContainers((ClusterScalingContext) context);
            return context;
        } catch (Exception e) {
            LOGGER.error("Error occurred while setting up containers for the cluster: {}", e.getMessage());
            throw new CloudbreakException(e);
        }
    }

    @Override
    public FlowContext upscaleCluster(FlowContext context) throws CloudbreakException {
        ClusterScalingContext scalingContext = (ClusterScalingContext) context;
        Stack stack = stackService.getById(scalingContext.getStackId());
        LOGGER.info("Upscaling Cluster. Context: {}", context);
        Set<String> hostNames = ambariClusterConnector.installAmbariNode(scalingContext.getStackId(), scalingContext.getHostGroupAdjustment(),
                scalingContext.getCandidates());
        if (!hostNames.isEmpty()) {
            updateInstanceMetadataAfterScaling(false, hostNames, stack);
        }
        return context;
    }

    @Override
    public FlowContext runClusterContainers(FlowContext context) throws CloudbreakException {
        ProvisioningContext provisioningContext = (ProvisioningContext) context;
        Stack stack = stackService.getById(provisioningContext.getStackId());
        MDCBuilder.buildMdcContext(stack);
        if (stack.getCluster() != null && stack.getCluster().isRequested()) {
            MDCBuilder.buildMdcContext(stack.getCluster());
            stackUpdater.updateStackStatus(stack.getId(), UPDATE_IN_PROGRESS, "Cluster installation has been started");
            LOGGER.debug("Launching Ambari cluster containers. Context: {}", context);
            containerRunner.runClusterContainers((ProvisioningContext) context);
            InstanceGroup gateway = stack.getGatewayInstanceGroup();
            InstanceMetaData gatewayInstance = gateway.getInstanceMetaData().iterator().next();
            provisioningContext = new ProvisioningContext.Builder()
                    .setDefaultParams(provisioningContext.getStackId(), provisioningContext.getCloudPlatform())
                    .setAmbariIp(gatewayInstance.getPublicIp())
                    .build();
        } else {
            LOGGER.info("The stack has started but there were no cluster request, yet. Won't install cluster now.");
            return provisioningContext;
        }
        return provisioningContext;
    }

    @Override
    public FlowContext downscaleCluster(FlowContext context) throws CloudbreakException {
        ClusterScalingContext scalingContext = (ClusterScalingContext) context;
        Stack stack = stackService.getById(scalingContext.getStackId());
        MDCBuilder.buildMdcContext(stack.getCluster());
        LOGGER.info("Downscaling cluster. Context: {}", context);
        stackUpdater.updateStackStatus(stack.getId(), UPDATE_IN_PROGRESS, String.format("Removing '%s' node(s) from the cluster.",
                scalingContext.getHostGroupAdjustment()));
        Set<String> hostNames = ambariClusterConnector.decommissionAmbariNodes(scalingContext.getStackId(), scalingContext.getHostGroupAdjustment(),
                scalingContext.getCandidates());
        if (!hostNames.isEmpty()) {
            updateInstanceMetadataAfterScaling(true, hostNames, stack);
        }
        HostGroup hostGroup = hostGroupService.getByClusterIdAndName(stack.getCluster().getId(), scalingContext.getHostGroupAdjustment().getHostGroup());
        StackScalingContext stackScalingContext = new StackScalingContext(scalingContext.getStackId(),
                scalingContext.getCloudPlatform(),
                scalingContext.getCandidates().size() * (-1),
                hostGroup.getInstanceGroup().getGroupName(),
                null,
                scalingContext.getScalingType(),
                null);
        return stackScalingContext;
    }

    @Override
    public FlowContext handleScalingFailure(FlowContext context) throws CloudbreakException {
        ClusterScalingContext scalingContext = (ClusterScalingContext) context;
        Stack stack = stackService.getById(scalingContext.getStackId());
        Cluster cluster = stack.getCluster();
        MDCBuilder.buildMdcContext(cluster);
        cluster.setStatus(UPDATE_FAILED);
        cluster.setStatusReason(scalingContext.getErrorReason());
        stackUpdater.updateStackCluster(stack.getId(), cluster);
        Integer scalingAdjustment = scalingContext.getHostGroupAdjustment().getScalingAdjustment();
        String statusMessage = scalingAdjustment > 0 ? "New node(s) could not be added to the cluster:" : "Node(s) could not be removed from the cluster:";
        stackUpdater.updateStackStatus(stack.getId(), AVAILABLE, statusMessage + scalingContext.getErrorReason());
        return context;
    }

    @Override
    public FlowContext resetAmbariCluster(FlowContext context) throws CloudbreakException {
        ProvisioningContext provisioningContext = (ProvisioningContext) context;
        Stack stack = stackService.getById(provisioningContext.getStackId());
        Cluster cluster = stack.getCluster();
        MDCBuilder.buildMdcContext(cluster);
        LOGGER.info("Reset Ambari Cluster. Context: {}", context);
        ambariClusterConnector.resetAmbariCluster(provisioningContext.getStackId());
        cluster.setStatus(Status.REQUESTED);
        stackUpdater.updateStackCluster(stack.getId(), cluster);
        return new ProvisioningContext.Builder()
                .withProvisioningContext(provisioningContext)
                .setAmbariIp(stack.getAmbariIp())
                .build();
    }

    public FlowContext enableSecurity(FlowContext context) throws CloudbreakException {
        ProvisioningContext provisioningContext = (ProvisioningContext) context;
        Stack stack = stackService.getById(provisioningContext.getStackId());
        if (stack.getCluster() == null) {
            LOGGER.debug("There is no cluster installed on the stack");
        } else {
            if (stack.getCluster().isSecure()) {
                LOGGER.debug("Cluster security is desired, trying to enable kerberos");
                updateClusterStatus(provisioningContext.getStackId(), UPDATE_IN_PROGRESS, "Enabling kerberos security.");
                securityService.enableKerberosSecurity(stack);
                updateClusterStatus(provisioningContext.getStackId(), AVAILABLE, "Kerberos security is enabled.");
            } else {
                LOGGER.debug("Cluster security is not requested");
            }
        }
        return provisioningContext;
    }

    @Override
    public FlowContext startRequested(FlowContext context) throws CloudbreakException {
        try {
            LOGGER.info("Stack is starting, set cluster state to: {}", START_REQUESTED);
            StackStatusUpdateContext updateContext = (StackStatusUpdateContext) context;
            Cluster cluster = clusterService.retrieveClusterByStackId(updateContext.getStackId());
            cluster.setStatus(START_REQUESTED);
            clusterService.updateCluster(cluster);
            LOGGER.debug("Start requested cluster is DONE.");
        } catch (Exception e) {
            LOGGER.error("Exception during the cluster start requested process: {}", e.getMessage());
            throw new CloudbreakException(e);
        }
        return context;
    }

    private void changeAmbariCredentials(String ambariIp, Cluster cluster) {
        String userName = cluster.getUserName();
        String password = cluster.getPassword();
        AmbariClient ambariClient = ambariClientProvider.getDefaultAmbariClient(ambariIp);
        if (ADMIN.equals(userName)) {
            if (!ADMIN.equals(password)) {
                ambariClient.changePassword(ADMIN, ADMIN, password, true);
            }
        } else {
            ambariClient.createUser(userName, password, true);
            ambariClient.deleteUser(ADMIN);
        }
    }

    private FlowContext updateStackAndClusterStatus(FlowContext context, Status clusterStatus) {
        StackStatusUpdateContext updateContext = (StackStatusUpdateContext) context;
        Cluster cluster = clusterService.retrieveClusterByStackId(updateContext.getStackId());
        MDCBuilder.buildMdcContext(cluster);
        cluster.setStatus(clusterStatus);
        clusterService.updateCluster(cluster);
        stackUpdater.updateStackStatus(updateContext.getStackId(), AVAILABLE, updateContext.getErrorReason());
        return context;
    }

    private void updateClusterStatus(Long stackId, Status status, String statusMessage) {
        Cluster cluster = clusterService.retrieveClusterByStackId(stackId);
        cluster.setStatusReason(statusMessage);
        cluster.setStatus(status);
        stackUpdater.updateStackCluster(stackId, cluster);
        eventService.fireCloudbreakEvent(stackId, status.name(), statusMessage);
    }


    private void updateInstanceMetadataAfterScaling(boolean decommission, Set<String> hostNames, Stack stack) {
        for (String hostName : hostNames) {
            if (decommission) {
                stackService.updateMetaDataStatus(stack.getId(), hostName, InstanceStatus.DECOMMISSIONED);
            } else {
                stackService.updateMetaDataStatus(stack.getId(), hostName, InstanceStatus.REGISTERED);
            }
        }
        String cause = decommission ? "Down" : "Up";
        String statusReason = String.format("%sscale of cluster finished successfully. AMBARI_IP:%s", cause, stack.getAmbariIp());
        stackUpdater.updateStackStatus(stack.getId(), AVAILABLE, statusReason);
    }
}
