package com.sequenceiq.cloudbreak.orchestrator.swarm.containers;

import static com.sequenceiq.cloudbreak.orchestrator.DockerContainer.BAYWATCH_SERVER;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.Ports;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.RestartPolicy;
import com.github.dockerjava.api.model.Volume;
import com.sequenceiq.cloudbreak.orchestrator.containers.ContainerBootstrap;
import com.sequenceiq.cloudbreak.orchestrator.swarm.DockerClientUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BaywatchServerBootstrap implements ContainerBootstrap {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaywatchServerBootstrap.class);

    private static final int ES_PORT = 9300;
    private static final int ES_TRANSPORT_PORT = 9200;
    private static final String ES_WORK_PATH = "/work";
    private static final String ES_DATA_PATH = "/tmp";

    private final DockerClient docker;
    private final String imageName;

    public BaywatchServerBootstrap(DockerClient docker, String imageName) {
        this.docker = docker;
        this.imageName = imageName;
    }

    @Override
    public Boolean call() throws Exception {
        HostConfig hostConfig = new HostConfig();
        hostConfig.setNetworkMode("host");
        hostConfig.setPrivileged(true);
        hostConfig.setRestartPolicy(RestartPolicy.alwaysRestart());
        try {
            String containerId = DockerClientUtil.createContainer(docker, docker.createContainerCmd(imageName)
                    .withName(BAYWATCH_SERVER.getName())
                    .withEnv(String.format("ES_CLUSTER_NAME=%s", "es-cluster-name"),
                            String.format("ES_DATA_PATH=%s", ES_DATA_PATH),
                            String.format("ES_WORK_PATH=%s", ES_WORK_PATH))
                    .withHostConfig(hostConfig));
            DockerClientUtil.startContainer(docker, docker.startContainerCmd(containerId)
                    .withPortBindings(
                            new PortBinding(new Ports.Binding("0.0.0.0", ES_PORT), new ExposedPort(ES_PORT)),
                            new PortBinding(new Ports.Binding("0.0.0.0", ES_TRANSPORT_PORT), new ExposedPort(ES_TRANSPORT_PORT)))
                    .withBinds(new Bind(ES_DATA_PATH, new Volume(ES_DATA_PATH)),
                            new Bind("/data", new Volume(ES_WORK_PATH)))
                    .withNetworkMode("host")
                    .withRestartPolicy(RestartPolicy.alwaysRestart()));
            LOGGER.info("Baywatch server container started successfully");
            return true;
        } catch (Exception ex) {
            LOGGER.info("Baywatch server container failed to start on node %s.");
            throw ex;
        }
    }
}
