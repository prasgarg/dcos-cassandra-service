package com.mesosphere.dcos.cassandra.common.util;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Contains util methods to find out if framework is being launched in a local setup or on a server
 */
public class LocalSetupUtils {
    /**
     * These are being exposed in mesos agents so that they can be randomly use for
     * EXECUTOR_API_PORT and JMX_PORT, these ports should be unique for each executor
     * and cassandra daemon.
     * Below variables specify the range of available ports, which are
     * randomly used by executor and cassandra daemon.
     */
    private static final int localExecutorApiPort = 9010;
    private static final int minExecutorApiPort = 9010;
    private static final int maxExecutorApiPort = 9040;

    private static final int minJmxPort = 7100;
    private static final int maxJmxPort = 7400;

    public static boolean executorCheckIfLocalSetUp() {
        Map<String, String> env = System.getenv();
        if(env.containsKey("EXECUTOR_API_PORT")) {
            int apiPort = Integer.parseInt(env.get("EXECUTOR_API_PORT"));
            if (apiPort >= minExecutorApiPort && apiPort <= maxExecutorApiPort)
                return true;
        }
        return false;
    }

    public static boolean schedulerCheckIfLocalSetUp(int apiPort) {
        if(apiPort == localExecutorApiPort)
            return true;
        return false;
    }

    public static int generateExecutorApiPort() {
        return ThreadLocalRandom.current().nextInt(minExecutorApiPort, maxExecutorApiPort + 1);
    }

    public static int generateJmxPort() {
        return ThreadLocalRandom.current().nextInt(minJmxPort, maxJmxPort + 1);
    }
}
