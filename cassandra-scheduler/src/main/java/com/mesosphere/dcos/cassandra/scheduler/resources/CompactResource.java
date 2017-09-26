package com.mesosphere.dcos.cassandra.scheduler.resources;

import com.codahale.metrics.annotation.Timed;
import com.mesosphere.dcos.cassandra.common.tasks.compact.CompactContext;
import com.mesosphere.dcos.cassandra.scheduler.plan.compact.CompactManager;

import javax.ws.rs.Consumes;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/v1/compact")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class CompactResource {

    private final ClusterTaskRunner<CompactRequest, CompactContext> runner;

    public CompactResource(final CompactManager manager) {
        this.runner = new ClusterTaskRunner<>(manager, "Compact");
    }

    @PUT
    @Timed
    @Path("start")
    public Response start(CompactRequest request) {
        return runner.start(request);
    }

    @PUT
    @Timed
    @Path("stop")
    public Response stop() {
        return runner.stop();
    }
}
