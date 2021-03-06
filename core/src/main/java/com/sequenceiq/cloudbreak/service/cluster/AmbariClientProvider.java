package com.sequenceiq.cloudbreak.service.cluster;

import org.springframework.stereotype.Service;

import com.sequenceiq.ambari.client.AmbariClient;
import com.sequenceiq.cloudbreak.domain.Cluster;

@Service
public class AmbariClientProvider {

    private static final String PORT = "80";
    private static final String ADMIN_PRINCIPAL = "/admin";

    /**
     * Create a new Ambari client. If the kerberos security is enabled
     * on the cluster this client won't be able to modify the cluster resources.
     *
     * @param ambariServerIP address of the Ambari server
     * @param ambariUserName username for the Ambari server
     * @param ambariPassword password for the Ambari server
     * @return client
     */
    public AmbariClient getAmbariClient(String ambariServerIP, String ambariUserName, String ambariPassword) {
        return new AmbariClient(ambariServerIP, PORT, ambariUserName, ambariPassword);
    }

    /**
     * Create a new Ambari client with the default user and password. If the kerberos security is enabled
     * on the cluster this client won't be able to modify the cluster resources.
     *
     * @param ambariAddress address of the Ambari server
     * @return client
     */
    public AmbariClient getDefaultAmbariClient(String ambariAddress) {
        return new AmbariClient(ambariAddress, PORT);
    }

    /**
     * Create a new Ambari client. If the kerberos security is enabled on the cluster it will
     * automatically set the kerberos session. Clusters with kerberos security requires to
     * set this session otherwise the client cannot modify any resources.
     *
     * @param cluster Cloudbreak cluster
     * @return client
     */
    public AmbariClient getSecureAmbariClient(Cluster cluster) {
        AmbariClient ambariClient = getAmbariClient(cluster.getAmbariIp(), cluster.getUserName(), cluster.getPassword());
        if (cluster.isSecure()) {
            setKerberosSession(ambariClient, cluster);
        }
        return ambariClient;
    }

    /**
     * Any Ambari client can be updated with a kerberos session to be able to modify
     * cluster resources in a kerberos enabled cluster.
     *
     * @param client client to be updated
     */
    public void setKerberosSession(AmbariClient client, Cluster cluster) {
        client.setKerberosSession(cluster.getKerberosAdmin() + ADMIN_PRINCIPAL, cluster.getKerberosPassword());
    }
}
