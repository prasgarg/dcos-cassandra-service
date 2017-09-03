/*
 * Copyright 2016 Mesosphere
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mesosphere.dcos.cassandra.executor.resources;

import static com.google.common.collect.Iterables.toArray;

import com.codahale.metrics.annotation.Counted;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.config.CassandraConfig;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraStatus;
import com.mesosphere.dcos.cassandra.executor.CassandraDaemonProcess;
import com.mesosphere.dcos.cassandra.executor.CassandraExecutor;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.mesos.Executor;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import java.lang.management.MemoryUsage;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * CassandraDaemonController implements the API for remote controll of the
 * Cassandra daemon process from the scheduler.
 */
@Path("/v1/cassandra")
@Produces(MediaType.APPLICATION_JSON)
public class CassandraDaemonController {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(CassandraDaemonController.class);

    private final CassandraExecutor executor;

    private final CassandraDaemonProcess getDaemon() {

        Optional<CassandraDaemonProcess> process = executor
                .getCassandraDaemon();

        if (!process.isPresent()) {
            throw new NotFoundException();
        } else return process.get();
    }

    /**
     * Constructs a new controller.
     * @param executor The Executor instance that will be controlled.
     */
    @Inject
    public CassandraDaemonController(Executor executor) {

        LOGGER.info("Setting executor to {}", executor);
        this.executor = (CassandraExecutor) executor;
        LOGGER.info("Set executor to {}", this.executor);
    }

    /**
     * Gets the status of the Cassandra process.
     * @return A CassandraStatus object containing the status of the
     * Cassandra process.
     */
    @GET
    @Counted
    @Path("/status")
    public CassandraStatus getStatus() {
        return getDaemon().getStatus();
    }

    @GET
    @Counted
    @Path("/unreachable")
    public List<String> getUnreachableNodes() {
        return getDaemon().getProbe().getUnreachableNodes();
    }

    /**
     * Gets the configuration of the Cassandra daemon.
     * @return A CassandraConfig object containing the configuration of the
     * Cassandra daemon.
     */
    @GET
    @Counted
    @Path("/configuration")
    public CassandraConfig getConfig() {

        return getDaemon().getTask().getConfig();
    }


    @GET
    @Counted
    @Path("/heapUsage")
    public String getHeapUsage() {
        NodeProbe probe = getDaemon().getProbe();
        long secondsUp = probe.getUptime() / 1000;

        // Memory usage
        MemoryUsage heapUsage = probe.getHeapMemoryUsage();
        double memUsed = (double) heapUsage.getUsed() / (1024 * 1024);
        double memMax = (double) heapUsage.getMax() / (1024 * 1024);
        long exception = probe.getStorageMetric("Exceptions");
        String load =  probe.getLoadString();

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("secondsUp", secondsUp);
        jsonObject.put("memUsed", memUsed);
        jsonObject.put("memMax", memMax);
        jsonObject.put("exceptions", exception);
        jsonObject.put("load", load);
        return jsonObject.toString();
    }

    @GET
    @Counted
    @Path("/compactionHistory")
    public List getCompactionHistory() {
        NodeProbe probe = getDaemon().getProbe();
        List<String> result = new LinkedList<>();
        TabularData tabularData = probe.getCompactionHistory();
        if (tabularData.isEmpty())
        {
            System.out.printf("There is no compaction history");
            return result;
        }

        String format = "%-41s%-19s%-29s%-26s%-15s%-15s%s%n";
        List<String> indexNames = tabularData.getTabularType().getIndexNames();
        result.add(String.format(format, toArray(indexNames, Object.class)));

        Set<?> values = tabularData.keySet();
        for (Object eachValue : values)
        {
            List<?> value = (List<?>) eachValue;
            result.add(String.format(format, toArray(value, Object.class)));
        }
        return result;
    }

    @GET
    @Counted
    @Path("/tpstats")
    public List getCacheStats() {
        NodeProbe probe = getDaemon().getProbe();
        Multimap<String, String> threadPools = probe.getThreadPools();
        List<String> result = new LinkedList<>();
        for (Map.Entry<String, String> tpool : threadPools.entries()) {
            result.add(String.format("%-25s%10s%10s%15s%10s%18s%n",
                            tpool.getValue(),
                            probe.getThreadPoolMetric(tpool.getKey(), tpool.getValue(), "ActiveTasks"),
                            probe.getThreadPoolMetric(tpool.getKey(), tpool.getValue(), "PendingTasks"),
                            probe.getThreadPoolMetric(tpool.getKey(), tpool.getValue(), "CompletedTasks"),
                            probe.getThreadPoolMetric(tpool.getKey(), tpool.getValue(), "CurrentlyBlockedTasks"),
                            probe.getThreadPoolMetric(tpool.getKey(), tpool.getValue(), "TotalBlockedTasks")));
        }

        for (Map.Entry<String, Integer> entry : probe.getDroppedMessages().entrySet())
            result.add(String.format("%-20s%10s%n", entry.getKey(), entry.getValue()));
        return result;
    }

}
