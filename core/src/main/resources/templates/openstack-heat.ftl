<#setting number_format="computer">
heat_template_version: 2014-10-16

description: >
  Heat OpenStack-native for Ambari

parameters:

  key_name:
    type: string
    description : Name of a KeyPair to enable SSH access to the instance
  tenant_id:
    type: string
    description : ID of the tenant
  image_id:
    type: string
    description: ID of the image
    default: Ubuntu 14.04 LTS amd64
  app_net_cidr:
    type: string
    description: app network address (CIDR notation)
    default: ${cbSubnet}
  public_net_id:
    type: string
    description: The ID of the public network. You will need to replace it with your DevStack public network ID
  app_network_id:
      type: string
      description: Fixed network id
      default: f331c0e3-435f-42d7-8b9f-8d2bb70ae091
  app_subnet_id:
    type: string
    description: Fixed subnet id
    default: ad8f7655-bed4-4356-b0ed-3f89c19157b4

resources:

  <#list agents as agent>
  <#assign metadata = agent.metadata?eval>
  <#assign instance_id = metadata.cb_instance_group_name?replace('_', '') + "_" + metadata.cb_instance_private_id>
  <#if agent.type == "GATEWAY">
     <#assign userdata = gatewayuserdata>
  <#elseif agent.type == "CORE">
     <#assign userdata = hostgroupuserdata>
  </#if>

  ambari_${instance_id}:
    type: OS::Nova::Server
    properties:
      image: { get_param: image_id }
      flavor: ${agent.flavor}
      key_name: { get_param: key_name }
      metadata: ${agent.metadata}
      networks:
        - port: { get_resource: ambari_app_port_${instance_id} }
      user_data:
        str_replace:
          template: |
${userdata}
          params:
            public_net_id: { get_param: public_net_id }

  ambari_app_port_${instance_id}:
      type: OS::Neutron::Port
      properties:
        network_id: { get_param: app_network_id }
        fixed_ips:
          - subnet_id: { get_param: app_subnet_id }

  </#list>

outputs:
  <#list agents as agent>
  <#assign m = agent.metadata?eval>
  <#assign instance_id = m.cb_instance_group_name?replace('_', '') + "_" + m.cb_instance_private_id>
  instance_uuid_${instance_id}:
    value: { get_attr: [ambari_${instance_id}, show, id] }
  </#list>