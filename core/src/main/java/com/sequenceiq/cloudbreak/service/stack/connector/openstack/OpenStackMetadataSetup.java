package com.sequenceiq.cloudbreak.service.stack.connector.openstack;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.openstack4j.api.OSClient;
import org.openstack4j.model.compute.Address;
import org.openstack4j.model.compute.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.sequenceiq.cloudbreak.domain.CloudPlatform;
import com.sequenceiq.cloudbreak.domain.InstanceGroup;
import com.sequenceiq.cloudbreak.domain.InstanceMetaData;
import com.sequenceiq.cloudbreak.domain.Resource;
import com.sequenceiq.cloudbreak.domain.ResourceType;
import com.sequenceiq.cloudbreak.domain.Stack;
import com.sequenceiq.cloudbreak.service.stack.connector.MetadataSetup;
import com.sequenceiq.cloudbreak.service.stack.flow.CoreInstanceMetaData;

@Component
public class OpenStackMetadataSetup implements MetadataSetup {

    private static final Logger LOGGER = LoggerFactory.getLogger(OpenStackMetadataSetup.class);

    @Autowired
    private OpenStackUtil openStackUtil;

    @Override
    public Set<CoreInstanceMetaData> collectMetadata(Stack stack) {
        OSClient osClient = openStackUtil.createOSClient(stack);
        Resource heatResource = stack.getResourceByType(ResourceType.HEAT_STACK);
        String heatStackId = heatResource.getResourceName();
        Set<CoreInstanceMetaData> instancesCoreMetadata = new HashSet<>();

        List<? extends org.openstack4j.model.heat.Resource> resources = osClient.heat().resources().list(stack.getName(), heatStackId);
        for (org.openstack4j.model.heat.Resource resource : resources) {
            LOGGER.info("Resource: {}", resource);
            LOGGER.info("Type: {}", resource.getType());
            String resourceId = resource.getPhysicalResourceId();
            LOGGER.info("ResourceID: {}", resourceId);
            if (resource.getType().contains("Server") || resource.getType().contains("server")) {
                Server server = osClient.compute().servers().get(resourceId);

                // Getting a private IP for any network
                String privateIp = null;
                Map<String, List<? extends Address>> adrMap = server.getAddresses().getAddresses();
                for (List<? extends Address> adrList : adrMap.values()) {
                    //just pick a private IP don't care which one if it has multiple IPs
                    privateIp = adrList.get(0).getAddr();
                }

                instancesCoreMetadata.add(new CoreInstanceMetaData(
                        resourceId,
                        privateIp,
                        privateIp,
                        server.getOsExtendedVolumesAttached().size(),
                        stack.getInstanceGroupByInstanceGroupName(server.getMetadata().get(HeatTemplateBuilder.CB_INSTANCE_GROUP_NAME))
                ));

            }
        }
        return instancesCoreMetadata;
    }

    @Override
    public Set<CoreInstanceMetaData> collectNewMetadata(Stack stack, Set<Resource> resourceList, final String instanceGroupName) {
        OSClient osClient = openStackUtil.createOSClient(stack);
        Resource heatResource = stack.getResourceByType(ResourceType.HEAT_STACK);
        String heatStackId = heatResource.getResourceName();
        org.openstack4j.model.heat.Stack heatStack = osClient.heat().stacks().getDetails(stack.getName(), heatStackId);
        List<Map<String, Object>> outputs = heatStack.getOutputs();
        Set<CoreInstanceMetaData> instancesCoreMetadata = new HashSet<>();
        for (InstanceGroup instanceGroup : stack.getInstanceGroups()) {
            if (instanceGroup.getGroupName().equals(instanceGroupName)) {
                for (Map<String, Object> map : outputs) {
                    String instanceUUID = (String) map.get("output_value");
                    Server server = osClient.compute().servers().get(instanceUUID);
                    Map<String, String> metadata = server.getMetadata();
                    String groupName = metadata.get(HeatTemplateBuilder.CB_INSTANCE_GROUP_NAME);
                    final String instanceId = openStackUtil.getInstanceId(instanceUUID, metadata);
                    boolean metadataExists = FluentIterable.from(instanceGroup.getInstanceMetaData()).anyMatch(new Predicate<InstanceMetaData>() {
                        @Override
                        public boolean apply(InstanceMetaData input) {
                            return input.getInstanceId().equals(instanceId);
                        }
                    });
                    if (!metadataExists && groupName.equals(instanceGroupName)) {
                        LOGGER.info("New instance added to metadata: [stack: '{}', instanceId: '{}']", stack.getId(), instanceId);
                        instancesCoreMetadata.add(createCoreMetaData(stack, server, groupName, instanceId));
                    }
                }
            }
        }
        return instancesCoreMetadata;
    }

    @Override
    public CloudPlatform getCloudPlatform() {
        return CloudPlatform.OPENSTACK;
    }

    private CoreInstanceMetaData createCoreMetaData(Stack stack, Server server, String instanceGroupName, String instanceId) {
        return new CoreInstanceMetaData(
                instanceId,
                server.getAddresses().getAddresses("app_network").get(0).getAddr(),
                server.getAddresses().getAddresses("app_network").get(1).getAddr(),
                server.getOsExtendedVolumesAttached().size(),
                stack.getInstanceGroupByInstanceGroupName(instanceGroupName)
        );
    }
}

