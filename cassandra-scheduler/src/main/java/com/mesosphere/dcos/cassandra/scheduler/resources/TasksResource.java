/*
 * Copyright 2015 Mesosphere
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

package com.mesosphere.dcos.cassandra.scheduler.resources;

import com.google.inject.Inject;
import com.google.protobuf.TextFormat;
import com.mesosphere.dcos.cassandra.common.config.ConfigurationManager;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraContainer;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
import com.mesosphere.dcos.cassandra.scheduler.CassandraScheduler;
import com.mesosphere.dcos.cassandra.scheduler.client.SchedulerClient;

import org.apache.commons.collections.map.HashedMap;
import org.apache.mesos.Log;
import org.apache.mesos.Protos;
import org.apache.mesos.config.ConfigStoreException;
import org.apache.mesos.dcos.Capabilities;
import org.glassfish.jersey.server.ManagedAsync;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.openmbean.TabularData;
import javax.ws.rs.*;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.lang.management.MemoryUsage;
import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;


@Path("/v1/nodes")
@Produces(MediaType.APPLICATION_JSON)
public class TasksResource {
    private static final Logger LOGGER = LoggerFactory.getLogger
            (TasksResource.class);
    private final Capabilities capabilities;
    private final CassandraState state;
    private final SchedulerClient client;
    private final ConfigurationManager configurationManager;

    @Inject
    public TasksResource(
            final Capabilities capabilities,
            final CassandraState state,
            final SchedulerClient client,
            final ConfigurationManager configurationManager) {
        this.capabilities = capabilities;
        this.state = state;
        this.client = client;
        this.configurationManager = configurationManager;
    }

    @GET
    @Path("/list")
    public List<String> list() {
        return new ArrayList<>(state.getDaemons().keySet());
    }

    @GET
    @Path("/{name}/status")
    @ManagedAsync
    public void getStatus(
        @PathParam("name") final String name,
        @Suspended final AsyncResponse response) {

        Optional<CassandraDaemonTask> taskOption =
            Optional.ofNullable(state.getDaemons().get(name));
        if (!taskOption.isPresent()) {
            response.resume(
                Response.status(Response.Status.NOT_FOUND).build());
        } else {
            CassandraDaemonTask task = taskOption.get();
            client.status(task.getHostname(), task.getExecutor().getApiPort()
            ).whenCompleteAsync((status, error) -> {
                LOGGER.info("prasgarg status api response received - ", status);
                if (status != null) {
                    response.resume(status);
                } else {
                    response.resume(Response.serverError());
                }
            });
        }
    }

    @GET
    @Path("/{name}/info")
    public DaemonInfo getInfo(@PathParam("name") final String name) {

        Optional<CassandraDaemonTask> taskOption =
            Optional.ofNullable(state.getDaemons().get(name));
        if (taskOption.isPresent()) {
            return DaemonInfo.create(taskOption.get());
        } else {
            throw new NotFoundException();
        }
    }

    @GET
    @Path("/info")
    public List getInfo() {
        return state.getDaemons().values().stream().map(task -> DaemonInfo.create(task)).collect(Collectors.toList());
    }

    @PUT
    @Path("/restart")
    public Response restart(@QueryParam("node") final String name) {
        Optional<CassandraDaemonTask> taskOption =
            Optional.ofNullable(state.getDaemons().get(name));
        if (taskOption.isPresent()) {
            CassandraDaemonTask task = taskOption.get();
            CassandraScheduler.getTaskKiller().killTask(task.getName(), false);
            return killResponse(Arrays.asList(task.getTaskInfo().getTaskId().getValue()));
        } else {
            return Response.serverError().build();
        }
    }

    @PUT
    @Path("/replace")
    public Response replace(@QueryParam("node") final String name)
        throws Exception {
        Optional<CassandraDaemonTask> taskOption =
            Optional.ofNullable(state.getDaemons().get(name));
        if (taskOption.isPresent()) {
            CassandraDaemonTask task = taskOption.get();
            final CassandraContainer movedContainer = state.moveCassandraContainer(task);
            state.update(movedContainer.getDaemonTask());
            state.update(movedContainer.getClusterTemplateTask());

            for (Protos.TaskInfo taskInfo : movedContainer.getTaskInfos()) {
                state.update(Protos.TaskStatus.newBuilder()
                        .setState(Protos.TaskState.TASK_FAILED)
                        .setTaskId(taskInfo.getTaskId())
                        .build());
            }

            LOGGER.info("Moved container ExecutorInfo: {}",
                    TextFormat.shortDebugString(movedContainer.getExecutorInfo()));
            CassandraScheduler.getTaskKiller().killTask(task.getName(), true);
            return killResponse(Arrays.asList(task.getTaskInfo().getTaskId().getValue()));
        } else {
            return Response.serverError().build();
        }
    }

    /**
     * Deprecated. See {@code ConnectionResource}.
     */
    @GET
    @Path("connect")
    @Deprecated
    public Map<String, Object> connect() throws ConfigStoreException {
        final ConnectionResource connectionResource =
                new ConnectionResource(capabilities, state, configurationManager);
        return connectionResource.connect();
    }

    /**
     * Deprecated. See {@code ConnectionResource}.
     */
    @GET
    @Path("connect/address")
    @Deprecated
    public List<String> connectAddress() {
        final ConnectionResource connectionResource =
                new ConnectionResource(capabilities, state, configurationManager);
        return connectionResource.connectAddress();
    }

    /**
     * Deprecated. See {@code ConnectionResource}.
     */
    @GET
    @Path("connect/dns")
    @Deprecated
    public List<String> connectDns() throws ConfigStoreException {
        final ConnectionResource connectionResource =
                new ConnectionResource(capabilities, state, configurationManager);
        return connectionResource.connectDns();
    }

    private static Response killResponse(List<String> taskIds) {
        return Response.ok(new JSONArray(taskIds).toString(), MediaType.APPLICATION_JSON).build();
    }


    @GET
    @Path("/list/unreachable")
    @ManagedAsync
    public void getStatus(
                    @Suspended final AsyncResponse response) {
        Set<String> nodeList = new HashSet<>();
        List<CompletableFuture> completableFutures  = new LinkedList<>();
        for(CassandraDaemonTask task : state.getDaemons().values()) {
            completableFutures.add((CompletableFuture<List>)client.unreachable(task.getHostname(), task.getExecutor().getApiPort()).whenCompleteAsync((status, error) -> {
                LOGGER.info("PRASMYTEST:Populating unreach :" + task.getHostname());
                if (!status.isEmpty())
                    nodeList.addAll(status);
            }));
        }
        CompletableFuture<Object> allDoneFuture = CompletableFuture.anyOf(completableFutures.toArray(new CompletableFuture[completableFutures.size()]));
        LOGGER.info("PRASMYTEST:Completed any unreach");
        allDoneFuture.thenAccept(
                        (result) -> {response.resume(nodeList);}
        );

    }


    @GET
    @Path("/list/heapUsage")
    @ManagedAsync
    public void getHeapStatus(
                    @Suspended final AsyncResponse response) {
        Map<String, HashMap> map = new HashedMap();

        List<CompletableFuture> completableFutures  = new LinkedList<>();
        for(CassandraDaemonTask task : state.getDaemons().values()) {
            completableFutures.add((CompletableFuture<HashMap>)client.heapUsage(task.getHostname(), task.getExecutor().getApiPort()).whenCompleteAsync((status, error) -> {
                map.put(task.getHostname(), status);
            }));
        }
        CompletableFuture<Void> allDoneFuture = CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[completableFutures.size()]));
        allDoneFuture.thenAccept(
                        (result) -> {response.resume(map);}
        );

    }

    @GET
    @Path("/list/compactionHistory")
    @ManagedAsync
    public void getCompactionHistory(
                    @Suspended final AsyncResponse response) {
        Map<String, List> map = new HashedMap();

        List<CompletableFuture> completableFutures  = new LinkedList<>();
        for(CassandraDaemonTask task : state.getDaemons().values()) {
            completableFutures.add((CompletableFuture<List>)client.compactionHistory(task.getHostname(), task.getExecutor().getApiPort()).whenCompleteAsync((status, error) -> {
                map.put(task.getHostname(), status);
            }));
        }

        CompletableFuture<Void> allDoneFuture = CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[completableFutures.size()]));

        allDoneFuture.thenAccept(
                        (result) -> {response.resume(map);}
        );

    }


    @GET
    @Path("/list/tpstats")
    @ManagedAsync
    public void getTpStats(
                    @Suspended final AsyncResponse response) {
        Map<String, List> map = new HashedMap();

        List<CompletableFuture> completableFutures  = new LinkedList<>();
        for(CassandraDaemonTask task : state.getDaemons().values()) {
            completableFutures.add((CompletableFuture<List>)client.tpstats(task.getHostname(), task.getExecutor().getApiPort()).whenCompleteAsync((status, error) -> {
                map.put(task.getHostname(), status);
            }));
        }

        CompletableFuture<Void> allDoneFuture = CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[completableFutures.size()]));

        allDoneFuture.thenAccept(
                        (result) -> {response.resume(map);}
        );

    }


    @GET
    @Path("/list/cfstats")
    @ManagedAsync
    public void getCfStats(
                    @Suspended final AsyncResponse response, @QueryParam("keyspace") final String keyspace,@QueryParam("table") final String table) {
        Map<String, List> map = new HashedMap();

        List<CompletableFuture> completableFutures  = new LinkedList<>();
        for(CassandraDaemonTask task : state.getDaemons().values()) {
            completableFutures.add((CompletableFuture<List>)client.cfstats(task.getHostname(), task.getExecutor().getApiPort(), keyspace, table).whenCompleteAsync((status, error) -> {
                map.put(task.getHostname(), status);
            }));
        }

        CompletableFuture<Void> allDoneFuture = CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[completableFutures.size()]));

        allDoneFuture.thenAccept(
                        (result) -> {response.resume(map);}
        );

    }


    @GET
    @Path("/list/cfhistograms")
    @ManagedAsync
    public void getCfHistograms(
                    @Suspended final AsyncResponse response, @QueryParam("keyspace") final String keyspace,@QueryParam("table") final String table) {
        Map<String, List> map = new HashedMap();

        List<CompletableFuture> completableFutures  = new LinkedList<>();
        for(CassandraDaemonTask task : state.getDaemons().values()) {
            completableFutures.add((CompletableFuture<List>)client.cfhistograms(task.getHostname(), task.getExecutor().getApiPort(), keyspace, table).whenCompleteAsync((status, error) -> {
                map.put(task.getHostname(), status);
            }));
        }

        CompletableFuture<Void> allDoneFuture = CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[completableFutures.size()]));

        allDoneFuture.thenAccept(
                        (result) -> {response.resume(map);}
        );

    }


    @GET
    @Path("/list/proxyhistograms")
    @ManagedAsync
    public void getProxyHistograms(
                    @Suspended final AsyncResponse response) {
        Map<String, List> map = new HashedMap();

        List<CompletableFuture> completableFutures  = new LinkedList<>();
        for(CassandraDaemonTask task : state.getDaemons().values()) {
            completableFutures.add((CompletableFuture<List>)client.proxyhistograms(task.getHostname(), task.getExecutor().getApiPort()).whenCompleteAsync((status, error) -> {
                map.put(task.getHostname(), status);
            }));
        }

        CompletableFuture<Void> allDoneFuture = CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[completableFutures.size()]));

        allDoneFuture.thenAccept(
                        (result) -> {response.resume(map);}
        );

    }




}
