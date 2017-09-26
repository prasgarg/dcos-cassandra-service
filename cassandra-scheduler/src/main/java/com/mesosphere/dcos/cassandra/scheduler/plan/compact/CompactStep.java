package com.mesosphere.dcos.cassandra.scheduler.plan.compact;

import com.mesosphere.dcos.cassandra.common.offer.CassandraOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.common.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.compact.CompactContext;
import com.mesosphere.dcos.cassandra.common.tasks.compact.CompactTask;
import com.mesosphere.dcos.cassandra.scheduler.plan.AbstractClusterTaskStep;

import java.util.Optional;
import org.apache.mesos.scheduler.plan.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactStep extends AbstractClusterTaskStep{

    private static final Logger LOGGER = LoggerFactory.getLogger(CompactStep.class);

    private final CompactContext context;

    public CompactStep(
            String daemon,
            CassandraState cassandraState,
            CassandraOfferRequirementProvider provider,
            CompactContext context) {
        super(daemon, CompactTask.nameForDaemon(daemon), cassandraState, provider);
        this.context = context;
    }

    @Override
    protected Optional<CassandraTask> getOrCreateTask() throws PersistenceException {
        CassandraDaemonTask daemonTask = cassandraState.getDaemons().get(daemon);
        if (daemonTask == null) {
            LOGGER.warn("Cassandra Daemon for compact does not exist");
            setStatus(Status.COMPLETE);
            return Optional.empty();
        }
        return Optional.of(cassandraState.getOrCreateCompact(daemonTask, context));
    }
}
