package com.sequenceiq.cloudbreak.service.stack.resource.azure.builders.instance;

import static com.sequenceiq.cloudbreak.EnvironmentVariableConfig.CB_GCP_AND_AZURE_USER_NAME;
import static com.sequenceiq.cloudbreak.domain.InstanceGroupType.isGateway;
import static com.sequenceiq.cloudbreak.service.stack.connector.azure.AzureStackUtil.NAME;
import static com.sequenceiq.cloudbreak.service.stack.connector.azure.AzureStackUtil.PORTS;
import static com.sequenceiq.cloudbreak.service.stack.connector.azure.AzureStackUtil.RESERVEDIPNAME;
import static com.sequenceiq.cloudbreak.service.stack.connector.azure.AzureStackUtil.SERVICENAME;
import static com.sequenceiq.cloudbreak.service.stack.connector.azure.AzureStackUtil.VIRTUAL_NETWORK_IP_ADDRESS;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import com.google.common.base.Optional;
import com.sequenceiq.cloud.azure.client.AzureClient;
import com.sequenceiq.cloudbreak.cloud.connector.CloudConnectorException;
import com.sequenceiq.cloudbreak.domain.AzureCredential;
import com.sequenceiq.cloudbreak.domain.AzureLocation;
import com.sequenceiq.cloudbreak.domain.AzureTemplate;
import com.sequenceiq.cloudbreak.domain.InstanceGroup;
import com.sequenceiq.cloudbreak.domain.Resource;
import com.sequenceiq.cloudbreak.domain.ResourceType;
import com.sequenceiq.cloudbreak.domain.Stack;
import com.sequenceiq.cloudbreak.repository.StackRepository;
import com.sequenceiq.cloudbreak.service.PollingService;
import com.sequenceiq.cloudbreak.service.network.NetworkUtils;
import com.sequenceiq.cloudbreak.service.stack.connector.azure.AzureResourceException;
import com.sequenceiq.cloudbreak.service.stack.connector.azure.AzureStackUtil;
import com.sequenceiq.cloudbreak.service.stack.flow.AzureInstanceStatusCheckerTask;
import com.sequenceiq.cloudbreak.service.stack.flow.AzureInstances;
import com.sequenceiq.cloudbreak.service.stack.resource.CreateResourceRequest;
import com.sequenceiq.cloudbreak.service.stack.resource.azure.AzureCreateResourceStatusCheckerTask;
import com.sequenceiq.cloudbreak.service.stack.resource.azure.AzureDeleteResourceStatusCheckerTask;
import com.sequenceiq.cloudbreak.service.stack.resource.azure.AzureResourcePollerObject;
import com.sequenceiq.cloudbreak.service.stack.resource.azure.AzureSimpleInstanceResourceBuilder;
import com.sequenceiq.cloudbreak.service.stack.resource.azure.model.AzureDeleteContextObject;
import com.sequenceiq.cloudbreak.service.stack.resource.azure.model.AzureProvisionContextObject;
import com.sequenceiq.cloudbreak.service.stack.resource.azure.model.AzureStartStopContextObject;

import groovyx.net.http.HttpResponseDecorator;
import groovyx.net.http.HttpResponseException;

@Component
@Order(3)
public class AzureVirtualMachineResourceBuilder extends AzureSimpleInstanceResourceBuilder {

    @Autowired
    private StackRepository stackRepository;

    @Autowired
    private PollingService<AzureInstances> azurePollingService;

    @Autowired
    private AzureStackUtil azureStackUtil;

    @Autowired
    private AzureCreateResourceStatusCheckerTask azureCreateResourceStatusCheckerTask;

    @Autowired
    private AzureDeleteResourceStatusCheckerTask azureDeleteResourceStatusCheckerTask;

    @Autowired
    private PollingService<AzureResourcePollerObject> azureResourcePollerObjectPollingService;

    @Autowired
    private AzureInstanceStatusCheckerTask azureInstanceStatusCheckerTask;

    @Override
    public Boolean create(final CreateResourceRequest createResourceRequest, final String region) throws Exception {
        AzureVirtualMachineCreateRequest aCSCR = (AzureVirtualMachineCreateRequest) createResourceRequest;
        try {
            HttpResponseDecorator virtualMachineResponse = (HttpResponseDecorator) aCSCR.getAzureClient().createVirtualMachine(aCSCR.getProps());
            AzureResourcePollerObject azureResourcePollerObject =
                    new AzureResourcePollerObject(aCSCR.getAzureClient(), aCSCR.getStack(), virtualMachineResponse);
            azureResourcePollerObjectPollingService.pollWithTimeout(azureCreateResourceStatusCheckerTask, azureResourcePollerObject,
                    POLLING_INTERVAL, MAX_POLLING_ATTEMPTS, MAX_FAILURE_COUNT);
        } catch (Exception ex) {
            throw checkException(ex);
        }
        return true;
    }

    @Override
    public List<Resource> buildResources(AzureProvisionContextObject provisionContextObject, int index, List<Resource> resources,
            Optional<InstanceGroup> instanceGroup) {
        Stack stack = stackRepository.findById(provisionContextObject.getStackId());
        String vmName = filterResourcesByType(resources, ResourceType.AZURE_CLOUD_SERVICE).get(0).getResourceName();
        return Arrays.asList(new Resource(resourceType(), vmName, stack, instanceGroup.orNull().getGroupName()));
    }

    @Override
    public CreateResourceRequest buildCreateRequest(AzureProvisionContextObject provisionContextObject, List<Resource> resources,
            List<Resource> buildResources, int index, Optional<InstanceGroup> instanceGroup, Optional<String> userData) throws Exception {
        try {
            Stack stack = stackRepository.findById(provisionContextObject.getStackId());
            AzureTemplate azureTemplate = (AzureTemplate) instanceGroup.orNull().getTemplate();
            AzureCredential azureCredential = (AzureCredential) stack.getCredential();
            byte[] encoded = Base64.encodeBase64(buildResources.get(0).getResourceName().getBytes());
            String label = new String(encoded);
            Map<String, Object> props = new HashMap<>();
            props.put(NAME, buildResources.get(0).getResourceName());
            props.put(DEPLOYMENTSLOT, PRODUCTION);
            props.put(LABEL, label);
            props.put(IMAGENAME, azureStackUtil.getOsImageName(stack.getCredential(), AzureLocation.valueOf(stack.getRegion()), stack.getImage()));
            props.put(IMAGESTOREURI, buildImageStoreUri(provisionContextObject.getCommonName(), buildResources.get(0).getResourceName()));
            props.put(HOSTNAME, buildResources.get(0).getResourceName());
            props.put(USERNAME, CB_GCP_AND_AZURE_USER_NAME);
            props.put(SSHPUBLICKEYFINGERPRINT, azureStackUtil.createX509Certificate(azureCredential).getSha1Fingerprint().toUpperCase());
            props.put(SSHPUBLICKEYPATH, String.format("/home/%s/.ssh/authorized_keys", CB_GCP_AND_AZURE_USER_NAME));
            props.put(AFFINITYGROUP, provisionContextObject.getCommonName());
            if (azureTemplate.getVolumeCount() > 0) {
                props.put(DISKS, generateDisksProperty(azureTemplate));
            }
            props.put(SERVICENAME, buildResources.get(0).getResourceName());
            props.put(SUBNETNAME, provisionContextObject.filterResourcesByType(ResourceType.AZURE_NETWORK).get(0).getResourceName());
            props.put(VIRTUAL_NETWORK_IP_ADDRESS, findNextValidIp(provisionContextObject));
            props.put(CUSTOMDATA, new String(Base64.encodeBase64(userData.orNull().getBytes())));
            props.put(VIRTUALNETWORKNAME, provisionContextObject.filterResourcesByType(ResourceType.AZURE_NETWORK).get(0).getResourceName());
            props.put(PORTS, NetworkUtils.getPorts(stack));
            props.put(VMTYPE, azureTemplate.getVmType().vmType().replaceAll(" ", ""));
            if (isGateway(instanceGroup.orNull().getInstanceGroupType())) {
                props.put(RESERVEDIPNAME, stack.getResourceByType(ResourceType.AZURE_RESERVED_IP).getResourceName());
            }
            return new AzureVirtualMachineCreateRequest(props, resources, buildResources, stack, instanceGroup.orNull());
        } catch (Exception e) {
            throw new CloudConnectorException(e);
        }
    }

    private List<Integer> generateDisksProperty(AzureTemplate azureTemplate) {
        List<Integer> disks = new ArrayList<>();
        for (int i = 0; i < azureTemplate.getVolumeCount(); i++) {
            disks.add(azureTemplate.getVolumeSize());
        }
        return disks;
    }

    private String findNextValidIp(AzureProvisionContextObject provisionContextObject) {
        Stack stack = stackRepository.findById(provisionContextObject.getStackId());
        String subnetCIDR = stack.getNetwork().getSubnetCIDR();
        String ip = azureStackUtil.getFirstAssignableIPOfSubnet(subnetCIDR);
        String lastAssignableIP = azureStackUtil.getLastAssignableIPOfSubnet(subnetCIDR);

        boolean found = false;
        while (!found && !ip.equals(lastAssignableIP)) {
            ip = azureStackUtil.getNextIPAddress(ip);
            found = provisionContextObject.putIfAbsent(ip);
        }
        return ip;
    }

    private String buildImageStoreUri(String commonName, String vmName) {
        return String.format("http://%s.blob.core.windows.net/vhd-store/%s.vhd", commonName, vmName);
    }

    @Override
    public Boolean delete(Resource resource, AzureDeleteContextObject aDCO, String region) throws Exception {
        Stack stack = stackRepository.findById(aDCO.getStackId());
        AzureCredential credential = (AzureCredential) stack.getCredential();
        try {
            Map<String, String> props = new HashMap<>();
            props.put(SERVICENAME, resource.getResourceName());
            props.put(NAME, resource.getResourceName());
            AzureClient azureClient = azureStackUtil.createAzureClient(credential);
            HttpResponseDecorator deleteVirtualMachineResult = (HttpResponseDecorator) azureClient.deleteVirtualMachine(props);
            AzureResourcePollerObject azureResourcePollerObject = new AzureResourcePollerObject(azureClient, stack, deleteVirtualMachineResult);
            azureResourcePollerObjectPollingService.pollWithTimeout(azureDeleteResourceStatusCheckerTask, azureResourcePollerObject,
                    POLLING_INTERVAL, MAX_POLLING_ATTEMPTS, MAX_FAILURE_COUNT);
        } catch (HttpResponseException ex) {
            httpResponseExceptionHandler(ex, resource.getResourceName(), stack.getOwner(), stack);
        } catch (Exception ex) {
            throw new AzureResourceException(ex);
        }
        return true;
    }

    @Override
    public Boolean start(AzureStartStopContextObject aSSCO, Resource resource, String region) {
        Stack stack = stackRepository.findById(aSSCO.getStack().getId());
        AzureCredential credential = (AzureCredential) stack.getCredential();
        boolean started = setStackState(aSSCO.getStack().getId(), resource, azureStackUtil.createAzureClient(credential), false);
        if (started) {
            azurePollingService.pollWithTimeout(
                    azureInstanceStatusCheckerTask,
                    new AzureInstances(aSSCO.getStack(), azureStackUtil.createAzureClient(credential), Arrays.asList(resource.getResourceName()), "Running"),
                    POLLING_INTERVAL,
                    MAX_ATTEMPTS_FOR_AMBARI_OPS,
                    MAX_FAILURE_COUNT);
            return true;
        }
        return false;
    }

    @Override
    public Boolean stop(AzureStartStopContextObject aSSCO, Resource resource, String region) {
        Stack stack = stackRepository.findById(aSSCO.getStack().getId());
        AzureCredential credential = (AzureCredential) stack.getCredential();
        boolean stopped = setStackState(aSSCO.getStack().getId(), resource, azureStackUtil.createAzureClient(credential), true);
        if (stopped) {
            azurePollingService.pollWithTimeout(
                    new AzureInstanceStatusCheckerTask(),
                    new AzureInstances(aSSCO.getStack(), azureStackUtil.createAzureClient(credential), Arrays.asList(resource.getResourceName()), "Suspended"),
                    POLLING_INTERVAL,
                    MAX_ATTEMPTS_FOR_AMBARI_OPS);
            return true;
        }
        return false;
    }

    private boolean setStackState(Long stackId, Resource resource, AzureClient azureClient, boolean stopped) {
        boolean result = true;
        try {
            Map<String, String> vmContext = createVMContext(resource.getResourceName());
            if (stopped) {
                azureClient.stopVirtualMachine(vmContext);
            } else {
                azureClient.startVirtualMachine(vmContext);
            }

        } catch (Exception e) {
            LOGGER.error(String.format("Failed to %s AZURE instances on stack: %s", stopped ? "stop" : "start", stackId));
            result = false;
        }
        return result;
    }

    private Map<String, String> createVMContext(String vmName) {
        Map<String, String> context = new HashMap<>();
        context.put(SERVICENAME, vmName);
        context.put(NAME, vmName);
        return context;
    }

    @Override
    public ResourceType resourceType() {
        return ResourceType.AZURE_VIRTUAL_MACHINE;
    }

    public class AzureVirtualMachineCreateRequest extends CreateResourceRequest {
        private Map<String, Object> props = new HashMap<>();
        private List<Resource> resources;
        private Stack stack;
        private InstanceGroup instanceGroup;

        public AzureVirtualMachineCreateRequest(Map<String, Object> props, List<Resource> resources, List<Resource> buildNames,
                Stack stack, InstanceGroup instanceGroup) {
            super(buildNames);
            this.stack = stack;
            this.props = props;
            this.resources = resources;
            this.instanceGroup = instanceGroup;
        }

        public Map<String, Object> getProps() {
            return props;
        }

        public AzureClient getAzureClient() {
            return azureStackUtil.createAzureClient((AzureCredential) stack.getCredential());
        }

        public Stack getStack() {
            return stack;
        }

        public List<Resource> getResources() {
            return resources;
        }

        public InstanceGroup getInstanceGroup() {
            return instanceGroup;
        }
    }

}
