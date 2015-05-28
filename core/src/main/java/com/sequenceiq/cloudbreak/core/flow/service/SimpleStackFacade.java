package com.sequenceiq.cloudbreak.core.flow.service;

import static com.sequenceiq.cloudbreak.domain.BillingStatus.BILLING_STOPPED;
import static com.sequenceiq.cloudbreak.domain.Status.AVAILABLE;
import static com.sequenceiq.cloudbreak.domain.Status.CREATE_FAILED;
import static com.sequenceiq.cloudbreak.domain.Status.CREATE_IN_PROGRESS;
import static com.sequenceiq.cloudbreak.domain.Status.DELETE_COMPLETED;
import static com.sequenceiq.cloudbreak.domain.Status.DELETE_FAILED;
import static com.sequenceiq.cloudbreak.domain.Status.DELETE_IN_PROGRESS;
import static com.sequenceiq.cloudbreak.domain.Status.START_FAILED;
import static com.sequenceiq.cloudbreak.domain.Status.START_IN_PROGRESS;
import static com.sequenceiq.cloudbreak.domain.Status.STOPPED;
import static com.sequenceiq.cloudbreak.domain.Status.STOP_FAILED;
import static com.sequenceiq.cloudbreak.domain.Status.STOP_IN_PROGRESS;
import static com.sequenceiq.cloudbreak.domain.Status.STOP_REQUESTED;
import static com.sequenceiq.cloudbreak.domain.Status.UPDATE_IN_PROGRESS;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.sequenceiq.cloudbreak.controller.json.HostGroupAdjustmentJson;
import com.sequenceiq.cloudbreak.core.CloudbreakException;
import com.sequenceiq.cloudbreak.core.bootstrap.service.ClusterBootstrapper;
import com.sequenceiq.cloudbreak.core.flow.context.ClusterScalingContext;
import com.sequenceiq.cloudbreak.core.flow.context.DefaultFlowContext;
import com.sequenceiq.cloudbreak.core.flow.context.FlowContext;
import com.sequenceiq.cloudbreak.core.flow.context.ProvisioningContext;
import com.sequenceiq.cloudbreak.core.flow.context.StackScalingContext;
import com.sequenceiq.cloudbreak.core.flow.context.StackStatusUpdateContext;
import com.sequenceiq.cloudbreak.core.flow.context.UpdateAllowedSubnetsContext;
import com.sequenceiq.cloudbreak.domain.BillingStatus;
import com.sequenceiq.cloudbreak.domain.CloudPlatform;
import com.sequenceiq.cloudbreak.domain.Cluster;
import com.sequenceiq.cloudbreak.domain.HostGroup;
import com.sequenceiq.cloudbreak.domain.HostMetadata;
import com.sequenceiq.cloudbreak.domain.InstanceGroupType;
import com.sequenceiq.cloudbreak.domain.OnFailureAction;
import com.sequenceiq.cloudbreak.domain.Resource;
import com.sequenceiq.cloudbreak.domain.Stack;
import com.sequenceiq.cloudbreak.domain.Subnet;
import com.sequenceiq.cloudbreak.logger.MDCBuilder;
import com.sequenceiq.cloudbreak.repository.RetryingStackUpdater;
import com.sequenceiq.cloudbreak.service.cluster.ClusterService;
import com.sequenceiq.cloudbreak.service.events.CloudbreakEventService;
import com.sequenceiq.cloudbreak.service.hostgroup.HostGroupService;
import com.sequenceiq.cloudbreak.service.stack.StackService;
import com.sequenceiq.cloudbreak.service.stack.connector.CloudPlatformConnector;
import com.sequenceiq.cloudbreak.service.stack.connector.UserDataBuilder;
import com.sequenceiq.cloudbreak.service.stack.event.ProvisionComplete;
import com.sequenceiq.cloudbreak.service.stack.flow.ConsulMetadataSetup;
import com.sequenceiq.cloudbreak.service.stack.flow.MetadataSetupService;
import com.sequenceiq.cloudbreak.service.stack.flow.ProvisioningService;
import com.sequenceiq.cloudbreak.service.stack.flow.StackScalingService;
import com.sequenceiq.cloudbreak.service.stack.flow.TerminationService;

@Service
public class SimpleStackFacade implements StackFacade {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleStackFacade.class);

    @Autowired
    private StackService stackService;

    @javax.annotation.Resource
    private Map<CloudPlatform, CloudPlatformConnector> cloudPlatformConnectors;

    @Autowired
    private CloudbreakEventService cloudbreakEventService;

    @Autowired
    private TerminationService terminationService;

    @Autowired
    private StackStartService stackStartService;

    @Autowired
    private StackStopService stackStopService;

    @Autowired
    private StackScalingService stackScalingService;

    @Autowired
    private MetadataSetupService metadataSetupService;

    @Autowired
    private UserDataBuilder userDataBuilder;

    @Autowired
    private HostGroupService hostGroupService;

    @Autowired
    private ClusterBootstrapper clusterBootstrapper;

    @Autowired
    private ConsulMetadataSetup consulMetadataSetup;

    @Autowired
    private ProvisioningService provisioningService;

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private RetryingStackUpdater stackUpdater;

    @Override
    public FlowContext bootstrapCluster(FlowContext context) throws CloudbreakException {
        ProvisioningContext actualContext = (ProvisioningContext) context;
        try {
            stackUpdater.updateStackStatus(actualContext.getStackId(), UPDATE_IN_PROGRESS);

            LOGGER.debug("Setting up bootstrap cluster [STARTED]. Context: {}", context);
            clusterBootstrapper.bootstrapCluster(actualContext);
            LOGGER.debug("Setting up bootstrap cluster [FINISHED]. Context: {}", context);
        } catch (Exception e) {
            LOGGER.error("Error occurred while bootstrapping container orchestrator: {}", e.getMessage());
            throw new CloudbreakException(e);
        }
        return context;
    }

    @Override
    public FlowContext setupConsulMetadata(FlowContext context) throws CloudbreakException {
        ProvisioningContext actualContext = (ProvisioningContext) context;
        try {
            stackUpdater.updateStackStatus(actualContext.getStackId(), UPDATE_IN_PROGRESS);
            Stack stack = stackService.getById(actualContext.getStackId());
            MDCBuilder.buildMdcContext(stack);

            LOGGER.debug("Setting up consul metadata [FLOW_STEP] [STARTED]. Context: {}", context);
            consulMetadataSetup.setupConsulMetadata(stack.getId());
            LOGGER.debug("Setting up consul metadata [FLOW_STEP] [FINISHED]. Context: {}", context);

        } catch (Exception e) {
            LOGGER.error("Exception during the consul metadata setup process.", e);
            throw new CloudbreakException(e);
        }
        return context;
    }

    @Override
    public FlowContext handleCreationFailure(FlowContext context) throws CloudbreakException {
        ProvisioningContext actualContext = (ProvisioningContext) context;
        final Stack stack = stackService.getById(actualContext.getStackId());
        MDCBuilder.buildMdcContext(stack);
        LOGGER.info("Stack creation failure. Context: {}", actualContext);
        try {
            if (!stack.isStackInDeletionPhase()) {
                final CloudPlatform cloudPlatform = actualContext.getCloudPlatform();
                if (!stack.getOnFailureActionAction().equals(OnFailureAction.ROLLBACK)) {
                    LOGGER.debug("Nothing to do. OnFailureAction {}", stack.getOnFailureActionAction());
                } else {
                    stackUpdater.updateStackStatus(stack.getId(), UPDATE_IN_PROGRESS,
                            String.format("Rollback is in progress, cause: %s", actualContext.getErrorReason()));
                    cloudPlatformConnectors.get(cloudPlatform).rollback(stack, stack.getResources());
                    cloudbreakEventService.fireCloudbreakEvent(stack.getId(), BILLING_STOPPED.name(), "Stack creation failed.");
                }
                stackUpdater.updateStackStatus(stack.getId(), CREATE_FAILED, actualContext.getErrorReason());
            }
            return new ProvisioningContext.Builder().setDefaultParams(stack.getId(), stack.cloudPlatform()).build();
        } catch (Exception ex) {
            LOGGER.error("Stack rollback failed on stack id : {}. Exception:", stack.getId(), ex);
            stackUpdater.updateStackStatus(stack.getId(), CREATE_FAILED, String.format("Rollback failed: %s", ex.getMessage()));
            throw new CloudbreakException(String.format("Stack rollback failed on {} stack: ", stack.getId(), ex));
        }
    }

    @Override
    public FlowContext start(FlowContext context) throws CloudbreakException {
        StackStatusUpdateContext actualContext = (StackStatusUpdateContext) context;
        try {
            stackUpdater.updateStackStatus(actualContext.getStackId(), START_IN_PROGRESS, "Cluster infrastructure is now starting.");
            Stack stack = stackService.getById(actualContext.getStackId());
            MDCBuilder.buildMdcContext(stack);

            LOGGER.debug("Setting up consul metadata [FLOW_STEP] [STARTED]. Context: {}", context);
            context = stackStartService.start(actualContext);
            LOGGER.debug("Setting up consul metadata [FLOW_STEP] [FINISHED]. Context: {}", context);

            stackUpdater.updateStackStatus(stack.getId(), AVAILABLE);
        } catch (Exception e) {
            LOGGER.error("Exception during the stack start process.", e);
            throw new CloudbreakException(e);
        }
        return context;
    }

    @Override
    public FlowContext stop(FlowContext context) throws CloudbreakException {
        StackStatusUpdateContext actualContext = (StackStatusUpdateContext) context;
        try {
            if (stackStopService.isStopPossible(actualContext)) {
                stackUpdater.updateStackStatus(actualContext.getStackId(), STOP_IN_PROGRESS, "Cluster infrastructure is stopping.");
                Stack stack = stackService.getById(actualContext.getStackId());
                MDCBuilder.buildMdcContext(stack);

                LOGGER.debug("Stopping stack [FLOW_STEP] [STARTED]. Context: {}", context);
                context = stackStopService.stop(actualContext);
                LOGGER.debug("Stopping stack [FLOW_STEP] [FINISHED]. Context: {}", context);

                stackUpdater.updateStackStatus(stack.getId(), STOPPED);

                cloudbreakEventService.fireCloudbreakEvent(stack.getId(), BILLING_STOPPED.name(), "Cluster infrastructure stopped.");
            }
        } catch (Exception e) {
            LOGGER.error("Exception during the stack stop process: {}", e.getMessage());
            throw new CloudbreakException(e);
        }
        return context;
    }

    @Override
    public FlowContext terminateStack(FlowContext context) throws CloudbreakException {
        DefaultFlowContext actualContext = (DefaultFlowContext) context;
        try {
            stackUpdater.updateStackStatus(actualContext.getStackId(), DELETE_IN_PROGRESS);
            Stack stack = stackService.getById(actualContext.getStackId());
            MDCBuilder.buildMdcContext(stack);

            LOGGER.debug("Terminating stack [FLOW_STEP] [STARTED]. Context: {}", context);
            terminationService.terminateStack(stack.getId(), actualContext.getCloudPlatform());
            LOGGER.debug("Terminating stack [FLOW_STEP] [FINISHED]. Context: {}", context);

            LOGGER.debug("Finalize termination stack [FLOW_STEP] [STARTED]. Context: {}", context);
            terminationService.finalizeTermination(stack.getId());
            LOGGER.debug("Finalize termination stack [FLOW_STEP] [FINISHED]. Context: {}", context);

            clusterService.updateClusterStatusByStackId(stack.getId(), DELETE_COMPLETED);
            stackUpdater.updateStackStatus(stack.getId(), DELETE_COMPLETED);

            cloudbreakEventService.fireCloudbreakEvent(stack.getId(), DELETE_COMPLETED.name(),
                    "The cluster and its infrastructure have successfully been terminated.");
            cloudbreakEventService.fireCloudbreakEvent(stack.getId(), BillingStatus.BILLING_STOPPED.name(),
                    "Billing stopped; the cluster and its infrastructure have been terminated.");
        } catch (Exception e) {
            LOGGER.error("Exception during the stack termination process: {}", e.getMessage());
            throw new CloudbreakException(e);
        }
        return context;
    }

    @Override
    public FlowContext addInstances(FlowContext context) throws CloudbreakException {
        StackScalingContext actualContext = (StackScalingContext) context;
        try {
            stackUpdater.updateStackStatus(actualContext.getStackId(), UPDATE_IN_PROGRESS);
            Stack stack = stackService.getById(actualContext.getStackId());
            MDCBuilder.buildMdcContext(stack);

            LOGGER.debug("Add instances to stack [FLOW_STEP] [STARTED]. Context: {}", context);
            Set<Resource> resources = stackScalingService.addInstances(stack.getId(), actualContext.getInstanceGroup(), actualContext.getScalingAdjustment());
            context = new StackScalingContext(stack.getId(), actualContext.getCloudPlatform(), actualContext.getScalingAdjustment(),
                    actualContext.getInstanceGroup(), resources, actualContext.getScalingType(), null);
            LOGGER.debug("Add instances to stack [FLOW_STEP] [FINISHED]. Context: {}", context);

        } catch (Exception e) {
            LOGGER.error("Exception during the upscaling of stack: {}", e.getMessage());
            throw new CloudbreakException(e);
        }
        return context;
    }

    @Override
    public FlowContext extendMetadata(FlowContext context) throws CloudbreakException {
        StackScalingContext actualCont = (StackScalingContext) context;
        Stack stack = stackService.getById(actualCont.getStackId());
        Cluster cluster = clusterService.retrieveClusterByStackId(stack.getId());
        MDCBuilder.buildMdcContext(stackService.getById(stack.getId()));

        LOGGER.debug("Add instances to stack [FLOW_STEP] [STARTED]. Context: {}", context);
        Set<String> upscaleCandidateAddresses = metadataSetupService.setupNewMetadata(stack.getId(), actualCont.getResources(), actualCont.getInstanceGroup());
        HostGroupAdjustmentJson hostGroupAdjustmentJson = new HostGroupAdjustmentJson();
        hostGroupAdjustmentJson.setWithStackUpdate(false);
        hostGroupAdjustmentJson.setScalingAdjustment(actualCont.getScalingAdjustment());
        if (stack.getCluster() != null) {
            HostGroup hostGroup = hostGroupService.getByClusterIdAndInstanceGroupName(cluster.getId(), actualCont.getInstanceGroup());
            hostGroupAdjustmentJson.setHostGroup(hostGroup.getName());
        }
        LOGGER.debug("Add instances to stack [FLOW_STEP] [FINISHED]. Context: {}", context);

        context = new StackScalingContext(stack.getId(), actualCont.getCloudPlatform(), actualCont.getScalingAdjustment(), actualCont.getInstanceGroup(),
                actualCont.getResources(), actualCont.getScalingType(), upscaleCandidateAddresses);
        return context;
    }

    @Override
    public FlowContext bootstrapNewNodes(FlowContext context) throws CloudbreakException {
        StackScalingContext actualContext = (StackScalingContext) context;
        try {
            Stack stack = stackService.getById(actualContext.getStackId());

            LOGGER.debug("Bootstrap new nodes stack [FLOW_STEP] [STARTED]. Context: {}", context);
            clusterBootstrapper.bootstrapNewNodes(actualContext);
            LOGGER.debug("Bootstrap new nodes stack [FLOW_STEP] [FINISHED]. Context: {}", context);

            stackUpdater.updateStackStatus(stack.getId(), AVAILABLE);
        } catch (Exception e) {
            LOGGER.error("Exception during the handling of munchausen setup: {}", e.getMessage());
            throw new CloudbreakException(e);
        }
        return context;
    }

    @Override
    public FlowContext extendConsulMetadata(FlowContext context) throws CloudbreakException {
        StackScalingContext actualContext = (StackScalingContext) context;
        try {
            Stack stack = stackService.getById(actualContext.getStackId());
            Cluster cluster = clusterService.retrieveClusterByStackId(stack.getId());
            MDCBuilder.buildMdcContext(stack);

            LOGGER.debug("Extend consul metadate new nodes stack [FLOW_STEP] [STARTED]. Context: {}", context);
            consulMetadataSetup.setupNewConsulMetadata(stack.getId(), actualContext.getUpscaleCandidateAddresses());
            HostGroupAdjustmentJson hostGroupAdjustmentJson = new HostGroupAdjustmentJson();
            hostGroupAdjustmentJson.setWithStackUpdate(false);
            hostGroupAdjustmentJson.setScalingAdjustment(actualContext.getScalingAdjustment());
            if (cluster != null) {
                HostGroup hostGroup = hostGroupService.getByClusterIdAndInstanceGroupName(cluster.getId(), actualContext.getInstanceGroup());
                hostGroupAdjustmentJson.setHostGroup(hostGroup.getName());
            }
            LOGGER.debug("Extend consul metadate new nodes stack [FLOW_STEP] [FINISHED]. Context: {}", context);

            context = new ClusterScalingContext(stack.getId(), actualContext.getCloudPlatform(),
                    hostGroupAdjustmentJson, actualContext.getUpscaleCandidateAddresses(), new ArrayList<HostMetadata>(), actualContext.getScalingType());
        } catch (Exception e) {
            LOGGER.error("Exception during the extend consul metadata phase: {}", e.getMessage());
            throw new CloudbreakException(e);
        }
        return context;
    }

    @Override
    public FlowContext downscaleStack(FlowContext context) throws CloudbreakException {
        StackScalingContext actualContext = (StackScalingContext) context;
        try {
            Stack stack = stackService.getById(actualContext.getStackId());
            MDCBuilder.buildMdcContext(stack);

            LOGGER.debug("Downscale stack [FLOW_STEP] [STARTED]. Context: {}", context);
            String statusMessage = "Removing '%s' instance(s) from the cluster infrastructure.";
            stackUpdater.updateStackStatus(stack.getId(), UPDATE_IN_PROGRESS);
            cloudbreakEventService
                    .fireCloudbreakEvent(stack.getId(), UPDATE_IN_PROGRESS.name(), String.format(statusMessage, actualContext.getScalingAdjustment()));
            stackScalingService.downscaleStack(stack.getId(), actualContext.getInstanceGroup(), actualContext.getScalingAdjustment());
            LOGGER.debug("Downscale stack [FLOW_STEP] [FINISHED]. Context: {}", context);

            stackUpdater.updateStackStatus(stack.getId(), AVAILABLE);
        } catch (Exception e) {
            LOGGER.error("Exception during the downscaling of stack: {}", e.getMessage());
            throw new CloudbreakException(e);
        }
        return context;
    }

    @Override
    public FlowContext stopRequested(FlowContext context) throws CloudbreakException {
        StackStatusUpdateContext actualContext = (StackStatusUpdateContext) context;
        try {
            Stack stack = stackService.getById(actualContext.getStackId());
            MDCBuilder.buildMdcContext(stack);

            LOGGER.debug("Stop requested stack [FLOW_STEP] [STARTED]. Context: {}", context);
            stackUpdater.updateStackStatus(stack.getId(), STOP_REQUESTED, "Stopping of cluster infrastructure has been requested.");
            LOGGER.debug("Stop requested stack [FLOW_STEP] [FINISHED]. Context: {}", context);
        } catch (Exception e) {
            LOGGER.error("Exception during the stack stop requested process: {}", e.getMessage());
            throw new CloudbreakException(e);
        }
        return context;
    }

    @Override
    public FlowContext provision(FlowContext context) throws CloudbreakException {
        ProvisioningContext actualContext = (ProvisioningContext) context;
        try {
            Date startDate = new Date();
            Stack stack = stackService.getById(actualContext.getStackId());

            LOGGER.debug("Provision stack [FLOW_STEP] [STARTED]. Context: {}", context);
            stackUpdater.updateStackStatus(stack.getId(), CREATE_IN_PROGRESS);
            ProvisionComplete provisionResult = provisioningService.buildStack(actualContext.getCloudPlatform(), stack, actualContext.getSetupProperties());
            Date endDate = new Date();
            LOGGER.debug("Provision stack [FLOW_STEP] [FINISHED]. Context: {}", context);

            long seconds = (endDate.getTime() - startDate.getTime()) / DateUtils.MILLIS_PER_SECOND;
            cloudbreakEventService.fireCloudbreakEvent(stack.getId(), AVAILABLE.name(), String.format("The creation of instratructure was %s sec", seconds));
            context = new ProvisioningContext.Builder()
                    .setDefaultParams(provisionResult.getStackId(), provisionResult.getCloudPlatform())
                    .setProvisionedResources(provisionResult.getResources())
                    .build();
        } catch (Exception e) {
            LOGGER.error("Exception during the stack stop requested process: {}", e.getMessage());
            throw new CloudbreakException(e);
        }
        return context;
    }

    @Override
    public FlowContext updateAllowedSubnets(FlowContext context) throws CloudbreakException {
        UpdateAllowedSubnetsContext actualContext = (UpdateAllowedSubnetsContext) context;
        try {
            Stack stack = stackService.getById(actualContext.getStackId());
            stackUpdater.updateStackStatus(stack.getId(), UPDATE_IN_PROGRESS);
            MDCBuilder.buildMdcContext(stack);

            LOGGER.debug("Subnet update stack [FLOW_STEP] [STARTED]. Context: {}", context);
            Map<InstanceGroupType, String> userdata = userDataBuilder.buildUserData(stack.cloudPlatform());
            stack.setAllowedSubnets(getNewSubnetList(stack, actualContext.getAllowedSubnets()));
            cloudPlatformConnectors.get(stack.cloudPlatform())
                    .updateAllowedSubnets(stack, userdata.get(InstanceGroupType.GATEWAY), userdata.get(InstanceGroupType.CORE));
            stackUpdater.updateStack(stack);
            LOGGER.debug("Subnet update stack [FLOW_STEP] [FINISHED]. Context: {}", context);

            stackUpdater.updateStackStatus(stack.getId(), AVAILABLE, "Security update successfully finished");
        } catch (Exception e) {
            Stack stack = stackService.getById(actualContext.getStackId());
            String msg = String.format("Failed to update security constraints with allowed subnets: %s", stack.getAllowedSubnets());
            if (stack != null && stack.isStackInDeletionPhase()) {
                msg = String.format("Failed to update security constraints with allowed subnets: %s; stack is already in deletion phase.",
                        stack.getAllowedSubnets());
            }
            LOGGER.error(msg, e);
            throw new CloudbreakException(e);
        }
        return context;
    }

    @Override
    public FlowContext handleUpdateAllowedSubnetsFailure(FlowContext context) throws CloudbreakException {
        UpdateAllowedSubnetsContext actualContext = (UpdateAllowedSubnetsContext) context;
        try {
            Stack stack = stackService.getById(actualContext.getStackId());
            MDCBuilder.buildMdcContext(stack);

            LOGGER.debug("Subnet update failed stack [FLOW_STEP] [STARTED]. Context: {}", context);
            stackUpdater.updateStackStatus(stack.getId(), AVAILABLE, String.format("Stack update failed. %s", actualContext.getErrorReason()));
            LOGGER.debug("Subnet update failed stack [FLOW_STEP] [FINISHED]. Context: {}", context);

        } catch (Exception e) {
            LOGGER.error("Exception during the handling of update allowed subnet failure: {}", e.getMessage());
            throw new CloudbreakException(e);
        }
        return context;
    }

    @Override
    public FlowContext handleScalingFailure(FlowContext context) throws CloudbreakException {
        try {
            Long id = null;
            String errorReason = null;
            if (context instanceof StackScalingContext) {
                StackScalingContext actualContext = (StackScalingContext) context;
                id = actualContext.getStackId();
                errorReason = actualContext.getErrorReason();
            } else if (context instanceof ClusterScalingContext) {
                ClusterScalingContext actualContext = (ClusterScalingContext) context;
                id = actualContext.getStackId();
                errorReason = actualContext.getErrorReason();
            }
            if (id != null) {
                Stack stack = stackService.getById(id);
                MDCBuilder.buildMdcContext(stack);

                LOGGER.debug("Scaling failed stack [FLOW_STEP] [STARTED]. Context: {}", context);
                stackUpdater.updateStackStatus(stack.getId(), AVAILABLE, "Stack update failed. " + errorReason);
                LOGGER.debug("Scaling failed stack [FLOW_STEP] [STARTED]. Context: {}", context);
            }
        } catch (Exception e) {
            LOGGER.error("Exception during the handling of stack scaling failure: {}", e.getMessage());
            throw new CloudbreakException(e);
        }
        return context;
    }

    @Override
    public FlowContext handleTerminationFailure(FlowContext context) throws CloudbreakException {
        DefaultFlowContext actualContext = (DefaultFlowContext) context;
        Stack stack = stackService.getById(actualContext.getStackId());
        MDCBuilder.buildMdcContext(stack);

        LOGGER.debug("Delete failed stack [FLOW_STEP] [STARTED]. Context: {}", context);
        stackUpdater.updateStackStatus(stack.getId(), DELETE_FAILED, actualContext.getErrorReason());
        LOGGER.debug("Delete failed stack [FLOW_STEP] [FINISHED]. Context: {}", context);

        return context;
    }

    @Override
    public FlowContext handleStatusUpdateFailure(FlowContext flowContext) throws CloudbreakException {
        StackStatusUpdateContext context = (StackStatusUpdateContext) flowContext;
        Stack stack = stackService.getById(context.getStackId());
        MDCBuilder.buildMdcContext(stack);
        if (context.isStart()) {
            LOGGER.debug("Start failed stack [FLOW_STEP] [STARTED]. Context: {}", context);
            stackUpdater.updateStackStatus(context.getStackId(), START_FAILED, context.getErrorReason());
            LOGGER.debug("Start failed stack [FLOW_STEP] [FINISHED]. Context: {}", context);
        } else {
            LOGGER.debug("Stop failed stack [FLOW_STEP] [STARTED]. Context: {}", context);
            stackUpdater.updateStackStatus(context.getStackId(), STOP_FAILED, context.getErrorReason());
            LOGGER.debug("Stop failed stack [FLOW_STEP] [FINISHED]. Context: {}", context);
        }
        return context;
    }

    private Set<Subnet> getNewSubnetList(Stack stack, List<Subnet> subnetList) {
        Set<Subnet> copy = new HashSet<>();
        for (Subnet subnet : stack.getAllowedSubnets()) {
            if (!subnet.isModifiable()) {
                copy.add(subnet);
                removeFromNewSubnetList(subnet, subnetList);
            }
        }
        for (Subnet subnet : subnetList) {
            copy.add(subnet);
        }
        return copy;
    }

    private void removeFromNewSubnetList(Subnet subnet, List<Subnet> subnetList) {
        Iterator<Subnet> iterator = subnetList.iterator();
        String cidr = subnet.getCidr();
        while (iterator.hasNext()) {
            Subnet next = iterator.next();
            if (next.getCidr().equals(cidr)) {
                iterator.remove();
            }
        }
    }
}
