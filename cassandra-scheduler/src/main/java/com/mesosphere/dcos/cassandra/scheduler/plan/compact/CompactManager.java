package com.mesosphere.dcos.cassandra.scheduler.plan.compact;

import com.mesosphere.dcos.cassandra.common.offer.ClusterTaskOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.common.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
import com.mesosphere.dcos.cassandra.common.tasks.ClusterTaskManager;
import com.mesosphere.dcos.cassandra.common.tasks.compact.CompactContext;
import com.mesosphere.dcos.cassandra.scheduler.resources.CompactRequest;
import org.apache.mesos.scheduler.plan.DefaultPhase;
import org.apache.mesos.scheduler.plan.Phase;
import org.apache.mesos.scheduler.plan.Step;
import org.apache.mesos.scheduler.plan.strategy.SerialStrategy;
import org.apache.mesos.state.StateStore;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class CompactManager extends ClusterTaskManager<CompactRequest, CompactContext>{
	static final String COMPACT_KEY = "compact";

    private final CassandraState cassandraState;
    private final ClusterTaskOfferRequirementProvider provider;

    @Inject
    public CompactManager(
            CassandraState cassandraState,
            ClusterTaskOfferRequirementProvider provider,
            StateStore stateStore) {
        super(stateStore, COMPACT_KEY, CompactContext.class);
        this.provider = provider;
        this.cassandraState = cassandraState;
        restore();
    }

    @Override
    protected CompactContext toContext(CompactRequest request) {
        return request.toContext(cassandraState);
    }

    @Override
    protected List<Phase> createPhases(CompactContext context) {
        final Set<String> nodes = new HashSet<>(context.getNodes());
        final List<String> daemons = new ArrayList<>(cassandraState.getDaemons().keySet());
        Collections.sort(daemons);
        List<Step> steps = daemons.stream()
                .filter(daemon -> nodes.contains(daemon))
                .map(daemon -> new CompactStep(daemon, cassandraState, provider, context))
                .collect(Collectors.toList());
        return Arrays.asList(new DefaultPhase("Compact", steps, new SerialStrategy<>(), Collections.emptyList()));
    }

    @Override
    protected void clearTasks() throws PersistenceException {
        cassandraState.remove(cassandraState.getCompactTasks().keySet());
    }
}
