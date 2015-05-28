package com.sequenceiq.cloudbreak.core.flow.service;

import static com.sequenceiq.cloudbreak.domain.Status.AVAILABLE;
import static com.sequenceiq.cloudbreak.domain.Status.CREATE_FAILED;
import static com.sequenceiq.cloudbreak.domain.Status.ENABLE_SECURITY_FAILED;
import static com.sequenceiq.cloudbreak.domain.Status.REQUESTED;
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
        ProvisioningContext actualContext = (ProvisioningContext) context;
        Stack stack = stackService.getById(actualContext.getStackId());
        Cluster cluster = clusterService.retrieveClusterByStackId(actualContext.getStackId());
        MDCBuilder.buildMdcContext(stack);
        if (cluster == null) {
            LOGGER.debug("There is no cluster installed on the stack, skipping start Ambari step");
        } else {
            MDCBuilder.buildMdcContext(cluster);

            stackUpdater.updateStackStatus(stack.getId(), UPDATE_IN_PROGRESS);
            clusterService.updateClusterStatusByStackId(stack.getId(), UPDATE_IN_PROGRESS);

            LOGGER.debug("Start ambari cluster [FLOW_STEP] [STARTED]. Context: {}", context);
            AmbariClient ambariClient = ambariClientProvider.getDefaultAmbariClient(actualContext.getAmbariIp());
            AmbariStartupPollerObject ambariStartupPollerObject = new AmbariStartupPollerObject(stack, actualContext.getAmbariIp(), ambariClient);
            PollingResult pollingResult = ambariStartupPollerObjectPollingService
                    .pollWithTimeout(ambariStartupListenerTask, ambariStartupPollerObject, POLLING_INTERVAL, MAX_POLLING_ATTEMPTS);
            LOGGER.debug("Start ambari cluster [FLOW_STEP] [FINISHED]. Context: {}", context);

            LOGGER.debug("Check ambari cluster [FLOW_STEP] [STARTED]. Context: {}", context);
            if (isSuccess(pollingResult)) {
                LOGGER.info("Ambari has successfully started! Polling result: {}", pollingResult);
            } else {
                LOGGER.info("Could not start Ambari. polling result: {},  Context: {}", pollingResult, context);
                throw new CloudbreakException(String.format("Could not start Ambari. polling result: '%s',  Context: '%s'", pollingResult, context));
            }
            LOGGER.debug("Check ambari cluster [FLOW_STEP] [FINISHED]. Context: {}", context);

            LOGGER.debug("Update ambari ip cluster [FLOW_STEP] [STARTED]. Context: {}", context);
            clusterService.updateAmbariIp(cluster.getId(), actualContext.getAmbariIp());
            LOGGER.debug("Update ambari ip cluster[FLOW_STEP] [FINISHED]. Context: {}", context);

            LOGGER.debug("Change ambari credential cluster [FLOW_STEP] [STARTED]. Context: {}", context);
            changeAmbariCredentials(actualContext.getAmbariIp(), cluster);
            LOGGER.debug("Change ambari credential cluster [FLOW_STEP] [FINISHED]. Context: {}", context);

        }
        return context;
    }

    @Override
    public FlowContext buildAmbariCluster(FlowContext context) throws Exception {
        ProvisioningContext actualContext = (ProvisioningContext) context;
        Stack stack = stackService.getById(actualContext.getStackId());
        Cluster cluster = clusterService.retrieveClusterByStackId(actualContext.getStackId());
        MDCBuilder.buildMdcContext(stack);
        if (cluster == null) {
            LOGGER.debug("There is no cluster installed on the stack, skipping build Ambari step");
        } else {
            MDCBuilder.buildMdcContext(cluster);

            stackUpdater.updateStackStatus(stack.getId(), UPDATE_IN_PROGRESS);

            LOGGER.debug("Build ambari cluster [FLOW_STEP] [STARTED]. Context: {}", context);
            cluster = ambariClusterConnector.buildAmbariCluster(stack);
            LOGGER.debug("Build ambari cluster [FLOW_STEP] [FINISHED]. Context: {}", context);

            clusterService.updateClusterStatusByStackId(stack.getId(), AVAILABLE);
            stackUpdater.updateStackStatus(stack.getId(), AVAILABLE);

            LOGGER.debug("Send success ambari install mail cluster [FLOW_STEP] [STARTED]. Context: {}", context);
            if (cluster.getEmailNeeded()) {
                emailSenderService.sendSuccessEmail(cluster.getOwner(), stack.getAmbariIp());
            }
            LOGGER.debug("Send success ambari install mail cluster [FLOW_STEP] [FINISHED]. Context: {}", context);
        }
        return context;
    }

    @Override
    public FlowContext startCluster(FlowContext context) throws CloudbreakException {
        StackStatusUpdateContext actualContext = (StackStatusUpdateContext) context;
        Stack stack = stackService.getById(actualContext.getStackId());
        Cluster cluster = clusterService.retrieveClusterByStackId(actualContext.getStackId());
        MDCBuilder.buildMdcContext(stack);
        if (cluster != null && cluster.isStartRequested()) {
            MDCBuilder.buildMdcContext(cluster);

            clusterService.updateClusterStatusByStackId(stack.getId(), START_IN_PROGRESS);
            stackUpdater.updateStackStatus(stack.getId(), UPDATE_IN_PROGRESS);

            LOGGER.debug("Start ambari cluster [FLOW_STEP] [STARTED]. Context: {}", context);
            ambariClusterConnector.startCluster(stack);
            LOGGER.debug("Start ambari cluster [FLOW_STEP] [FINISHED]. Context: {}", context);

            cluster.setUpSince(new Date().getTime());
            clusterService.updateCluster(cluster);
            clusterService.updateClusterStatusByStackId(stack.getId(), AVAILABLE);
            stackUpdater.updateStackStatus(stack.getId(), AVAILABLE);
        } else {
            LOGGER.info("Cluster start has not been requested, start cluster later.");
        }
        return context;
    }

    @Override
    public FlowContext stopCluster(FlowContext context) throws CloudbreakException {
        StackStatusUpdateContext actualContext = (StackStatusUpdateContext) context;
        Stack stack = stackService.getById(actualContext.getStackId());
        Cluster cluster = clusterService.retrieveClusterByStackId(actualContext.getStackId());
        MDCBuilder.buildMdcContext(cluster);

        Status stackStatus = stack.getStatus();
        stackUpdater.updateStackStatus(stack.getId(), UPDATE_IN_PROGRESS);
        clusterService.updateClusterStatusByStackId(stack.getId(), STOP_IN_PROGRESS);

        LOGGER.debug("Stop ambari cluster [FLOW_STEP] [STARTED]. Context: {}", context);
        ambariClusterConnector.stopCluster(stack);
        LOGGER.debug("Stop ambari cluster [FLOW_STEP] [FINISHED]. Context: {}", context);

        stackUpdater.updateStackStatus(stack.getId(), stackStatus);
        clusterService.updateClusterStatusByStackId(stack.getId(), STOPPED);
        eventService.fireCloudbreakEvent(stack.getId(), STOPPED.name(), "Services have been stopped successfully.");

        return context;
    }

    @Override
    public FlowContext addClusterContainers(FlowContext context) throws CloudbreakException {
        ClusterScalingContext actualContext = (ClusterScalingContext) context;
        Stack stack = stackService.getById(actualContext.getStackId());
        MDCBuilder.buildMdcContext(stack);

        stackUpdater.updateStackStatus(stack.getId(), UPDATE_IN_PROGRESS);

        LOGGER.debug("Add cluster containers ambari cluster [FLOW_STEP] [STARTED]. Context: {}", context);
        containerRunner.addClusterContainers(actualContext);
        LOGGER.debug("Add cluster containers ambari cluster [FLOW_STEP] [FINISHED]. Context: {}", context);

        return context;
    }

    @Override
    public FlowContext upscaleCluster(FlowContext context) throws CloudbreakException {
        ClusterScalingContext actualContext = (ClusterScalingContext) context;
        Stack stack = stackService.getById(actualContext.getStackId());
        Cluster cluster = clusterService.retrieveClusterByStackId(actualContext.getStackId());
        MDCBuilder.buildMdcContext(cluster);

        stackUpdater.updateStackStatus(stack.getId(), UPDATE_IN_PROGRESS);

        LOGGER.debug("Upscale ambari cluster [FLOW_STEP] [STARTED]. Context: {}", context);
        Set<String> hostNames = ambariClusterConnector.installAmbariNode(stack.getId(), actualContext.getHostGroupAdjustment(), actualContext.getCandidates());
        updateInstanceMetadataAfterScaling(false, hostNames, stack);
        String statusReason = String.format("Upscale of cluster finished successfully. AMBARI_IP:%s", stack.getAmbariIp());
        LOGGER.debug("Upscale ambari cluster [FLOW_STEP] [FINISHED]. Context: {}", context);

        eventService.fireCloudbreakEvent(stack.getId(), AVAILABLE.name(), statusReason);
        stackUpdater.updateStackStatus(stack.getId(), AVAILABLE);
        clusterService.updateClusterStatusByStackId(stack.getId(), AVAILABLE);

        return context;
    }

    @Override
    public FlowContext runClusterContainers(FlowContext context) throws CloudbreakException {
        ProvisioningContext actualContext = (ProvisioningContext) context;
        Stack stack = stackService.getById(actualContext.getStackId());
        Cluster cluster = clusterService.retrieveClusterByStackId(stack.getId());
        MDCBuilder.buildMdcContext(stack);
        if (stack.getCluster() != null && cluster.isRequested()) {
            MDCBuilder.buildMdcContext(cluster);

            stackUpdater.updateStackStatus(stack.getId(), UPDATE_IN_PROGRESS);

            LOGGER.debug("Run cluster containers on ambari cluster [FLOW_STEP] [STARTED]. Context: {}", context);
            containerRunner.runClusterContainers(actualContext);
            InstanceGroup gateway = stack.getGatewayInstanceGroup();
            InstanceMetaData gatewayInstance = gateway.getInstanceMetaData().iterator().next();
            LOGGER.debug("Run cluster containers on ambari cluster [FLOW_STEP] [FINISHED]. Context: {}", context);

            context = new ProvisioningContext.Builder()
                    .setDefaultParams(stack.getId(), actualContext.getCloudPlatform())
                    .setAmbariIp(gatewayInstance.getPublicIp())
                    .build();
        } else {
            LOGGER.info("The stack has started but there were no cluster request, yet. Won't install cluster now.");
            return actualContext;
        }
        return context;
    }

    @Override
    public FlowContext downscaleCluster(FlowContext context) throws CloudbreakException {
        ClusterScalingContext actualContext = (ClusterScalingContext) context;
        Stack stack = stackService.getById(actualContext.getStackId());
        Cluster cluster = clusterService.retrieveClusterByStackId(stack.getId());
        MDCBuilder.buildMdcContext(cluster);

        stackUpdater.updateStackStatus(stack.getId(), UPDATE_IN_PROGRESS);
        clusterService.updateClusterStatusByStackId(stack.getId(), UPDATE_IN_PROGRESS);
        eventService.fireCloudbreakEvent(stack.getId(), UPDATE_IN_PROGRESS.name(),
                String.format("Removing '%s' node(s) from the cluster.", actualContext.getHostGroupAdjustment()));

        LOGGER.debug("Downscale ambari cluster [FLOW_STEP] [STARTED]. Context: {}", context);
        Set<String> hostNames = ambariClusterConnector
                .decommissionAmbariNodes(stack.getId(), actualContext.getHostGroupAdjustment(), actualContext.getCandidates());
        updateInstanceMetadataAfterScaling(true, hostNames, stack);
        HostGroup hostGroup = hostGroupService.getByClusterIdAndName(cluster.getId(), actualContext.getHostGroupAdjustment().getHostGroup());
        LOGGER.debug("Downscale ambari cluster [FLOW_STEP] [FINISHED]. Context: {}", context);

        stackUpdater.updateStackStatus(stack.getId(), AVAILABLE);
        clusterService.updateClusterStatusByStackId(stack.getId(), AVAILABLE);
        String statusReason = String.format("Downscale of cluster finished successfully. AMBARI_IP:%s", stack.getAmbariIp());
        eventService.fireCloudbreakEvent(stack.getId(), AVAILABLE.name(), statusReason);

        context = new StackScalingContext(stack.getId(),
                actualContext.getCloudPlatform(), actualContext.getCandidates().size() * (-1), hostGroup.getInstanceGroup().getGroupName(),
                null, actualContext.getScalingType(), null);
        return context;
    }

    @Override
    public FlowContext resetAmbariCluster(FlowContext context) throws CloudbreakException {
        ProvisioningContext actualContext = (ProvisioningContext) context;
        Stack stack = stackService.getById(actualContext.getStackId());
        Cluster cluster = clusterService.retrieveClusterByStackId(stack.getId());
        MDCBuilder.buildMdcContext(cluster);

        LOGGER.debug("Reset ambari cluster [FLOW_STEP] [STARTED]. Context: {}", context);
        ambariClusterConnector.resetAmbariCluster(stack.getId());
        LOGGER.debug("Reset ambari cluster [FLOW_STEP] [FINISHED]. Context: {}", context);

        clusterService.updateClusterStatusByStackId(stack.getId(), REQUESTED);

        context = new ProvisioningContext.Builder()
                .withProvisioningContext(actualContext)
                .setAmbariIp(stack.getAmbariIp())
                .build();
        return context;
    }

    public FlowContext enableSecurity(FlowContext context) throws CloudbreakException {
        ProvisioningContext actualContext = (ProvisioningContext) context;
        Stack stack = stackService.getById(actualContext.getStackId());
        Cluster cluster = clusterService.retrieveClusterByStackId(stack.getId());
        MDCBuilder.buildMdcContext(stack);
        if (cluster == null) {
            LOGGER.debug("There is no cluster installed on the stack");
        } else {
            MDCBuilder.buildMdcContext(cluster);

            stackUpdater.updateStackStatus(stack.getId(), UPDATE_IN_PROGRESS);
            clusterService.updateClusterStatusByStackId(stack.getId(), UPDATE_IN_PROGRESS);

            LOGGER.debug("Enable security on ambari cluster [FLOW_STEP] [STARTED]. Context: {}", context);
            if (cluster.isSecure()) {
                securityService.enableKerberosSecurity(stack);
            } else {
                LOGGER.debug("Cluster security is not requested");
            }
            LOGGER.debug("Enable security on ambari cluster [FLOW_STEP] [FINISHED]. Context: {}", context);

            clusterService.updateClusterStatusByStackId(stack.getId(), AVAILABLE);
            stackUpdater.updateStackStatus(stack.getId(), AVAILABLE);
        }
        return context;
    }

    @Override
    public FlowContext startRequested(FlowContext context) throws CloudbreakException {
        StackStatusUpdateContext actualContext = (StackStatusUpdateContext) context;
        Stack stack = stackService.getById(actualContext.getStackId());
        MDCBuilder.buildMdcContext(stack);

        LOGGER.debug("Start requested on ambari cluster [FLOW_STEP] [STARTED]. Context: {}", context);
        clusterService.updateClusterStatusByStackId(stack.getId(), START_REQUESTED);
        LOGGER.debug("Start requested on ambari cluster [FLOW_STEP] [FINISHED]. Context: {}", context);

        return context;
    }

    @Override
    public FlowContext handleStartFailure(FlowContext context) throws CloudbreakException {
        StackStatusUpdateContext actualContext = (StackStatusUpdateContext) context;
        Stack stack = stackService.getById(actualContext.getStackId());
        Cluster cluster = clusterService.retrieveClusterByStackId(actualContext.getStackId());
        MDCBuilder.buildMdcContext(cluster);

        LOGGER.debug("Start failed on ambari cluster [FLOW_STEP] [STARTED]. Context: {}", context);
        clusterService.updateClusterStatusByStackId(stack.getId(), START_FAILED);
        stackUpdater.updateStackStatus(actualContext.getStackId(), AVAILABLE, actualContext.getErrorReason());
        LOGGER.debug("Start failed on ambari cluster [FLOW_STEP] [FINISHED]. Context: {}", context);

        return context;
    }

    @Override
    public FlowContext handleStopFailure(FlowContext context) throws CloudbreakException {
        StackStatusUpdateContext actualContext = (StackStatusUpdateContext) context;
        Stack stack = stackService.getById(actualContext.getStackId());
        Cluster cluster = clusterService.retrieveClusterByStackId(actualContext.getStackId());
        MDCBuilder.buildMdcContext(cluster);

        LOGGER.debug("Stop failed on ambari cluster [FLOW_STEP] [STARTED]. Context: {}", context);
        clusterService.updateClusterStatusByStackId(stack.getId(), STOP_FAILED);
        stackUpdater.updateStackStatus(stack.getId(), AVAILABLE, actualContext.getErrorReason());
        LOGGER.debug("Stop failed on ambari cluster [FLOW_STEP] [FINISHED]. Context: {}", context);

        return context;
    }

    @Override
    public FlowContext handleClusterCreationFailure(FlowContext context) throws CloudbreakException {
        ProvisioningContext actualContext = (ProvisioningContext) context;
        Stack stack = stackService.getById(actualContext.getStackId());
        Cluster cluster = clusterService.retrieveClusterByStackId(actualContext.getStackId());
        MDCBuilder.buildMdcContext(cluster);

        LOGGER.debug("Creation failed on ambari cluster [FLOW_STEP] [STARTED]. Context: {}", context);
        clusterService.updateClusterStatusByStackId(stack.getId(), CREATE_FAILED, actualContext.getErrorReason());
        stackUpdater.updateStackStatus(stack.getId(), AVAILABLE);
        LOGGER.debug("Creation failed on ambari cluster [FLOW_STEP] [FINISHED]. Context: {}", context);

        eventService.fireCloudbreakEvent(stack.getId(), CREATE_FAILED.name(), actualContext.getErrorReason());

        LOGGER.debug("Sending failed mail ambari cluster [FLOW_STEP] [STARTED]. Context: {}", context);
        if (cluster.getEmailNeeded()) {
            emailSenderService.sendFailureEmail(cluster.getOwner());
        }
        LOGGER.debug("Sending failed mail ambari cluster [FLOW_STEP] [FINISHED]. Context: {}", context);

        return actualContext;
    }

    @Override
    public FlowContext handleSecurityEnableFailure(FlowContext context) throws CloudbreakException {
        ProvisioningContext actualContext = (ProvisioningContext) context;
        Stack stack = stackService.getById(actualContext.getStackId());
        Cluster cluster = clusterService.retrieveClusterByStackId(stack.getId());
        MDCBuilder.buildMdcContext(cluster);

        LOGGER.debug("Enable security failed on ambari cluster [FLOW_STEP] [STARTED]. Context: {}", context);
        clusterService.updateClusterStatusByStackId(stack.getId(), ENABLE_SECURITY_FAILED, actualContext.getErrorReason());
        stackUpdater.updateStackStatus(stack.getId(), AVAILABLE);
        LOGGER.debug("Enable security failed on ambari cluster [FLOW_STEP] [FINISHED]. Context: {}", context);

        eventService.fireCloudbreakEvent(stack.getId(), ENABLE_SECURITY_FAILED.name(), actualContext.getErrorReason());

        LOGGER.debug("Sending failed mail enable security on ambari cluster [FLOW_STEP] [STARTED]. Context: {}", context);
        if (cluster.getEmailNeeded()) {
            emailSenderService.sendFailureEmail(cluster.getOwner());
        }
        LOGGER.debug("Sending failed mail enable security on ambari cluster [FLOW_STEP] [FINISHED]. Context: {}", context);

        return context;
    }

    @Override
    public FlowContext handleScalingFailure(FlowContext context) throws CloudbreakException {
        ClusterScalingContext actualContext = (ClusterScalingContext) context;
        Stack stack = stackService.getById(actualContext.getStackId());
        Cluster cluster = clusterService.retrieveClusterByStackId(stack.getId());
        MDCBuilder.buildMdcContext(cluster);

        LOGGER.debug("Scaling failed ambari cluster [FLOW_STEP] [STARTED]. Context: {}", context);
        clusterService.updateClusterStatusByStackId(stack.getId(), UPDATE_FAILED, actualContext.getErrorReason());
        Integer scalingAdjustment = actualContext.getHostGroupAdjustment().getScalingAdjustment();
        String statusMessage = scalingAdjustment > 0 ? "New node(s) could not be added to the cluster:" : "Node(s) could not be removed from the cluster:";
        stackUpdater.updateStackStatus(stack.getId(), AVAILABLE, String.format("%s %s", statusMessage, actualContext.getErrorReason()));
        LOGGER.debug("Scaling failed ambari cluster [FLOW_STEP] [FINISHED]. Context: {}", context);

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

    private void updateInstanceMetadataAfterScaling(boolean decommission, Set<String> hostNames, Stack stack) {
        for (String hostName : hostNames) {
            if (decommission) {
                stackService.updateMetaDataStatus(stack.getId(), hostName, InstanceStatus.DECOMMISSIONED);
            } else {
                stackService.updateMetaDataStatus(stack.getId(), hostName, InstanceStatus.REGISTERED);
            }
        }
    }
}
