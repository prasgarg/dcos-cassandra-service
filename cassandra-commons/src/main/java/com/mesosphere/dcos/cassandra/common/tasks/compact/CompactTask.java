package com.mesosphere.dcos.cassandra.common.tasks.compact;

import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraData;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTaskStatus;
import org.apache.mesos.Protos;
import org.apache.mesos.offer.TaskUtils;

import java.util.Optional;

/**
 * CompactTask performs single data center
 * compaction on a node. In order to successfully execute, a
 * CassandraDaemonTask must be running on the slave. If the indicated key
 * spaces are empty, all non-system key spaces will be compacted. If the column
 * families are empty, all column families for the selected key spaces will
 * be compacted.
 */
public class CompactTask extends CassandraTask {

    /**
     * Prefix for the name of CompactTasks
     */
    public static final String NAME_PREFIX = "compact-";


    /**
     * Gets the name of a CompactTask for a CassandraDaemonTask.
     *
     * @param daemonName The name of the CassandraDaemonTask.
     * @return The name of the  CompactTask for daemonName.
     */
    public static final String nameForDaemon(final String daemonName) {
        return NAME_PREFIX + daemonName;
    }

    /**
     * Gets the name of a CompactTask for a CassandraDaemonTask.
     *
     * @param daemon The CassandraDaemonTask for which the snapshot will be
     *               uploaded.
     * @return The name of the  CompactTask for daemon.
     */
    public static final String nameForDaemon(final CassandraDaemonTask daemon) {
        return nameForDaemon(daemon.getName());
    }

    public static CompactTask parse(final Protos.TaskInfo info) {
        return new CompactTask(info);
    }

    public static CompactTask create(
            final Protos.TaskInfo template,
            final CassandraDaemonTask daemon,
            final CompactContext context) {

        CassandraData data = CassandraData.createCompactData("", context);

        String name = nameForDaemon(daemon);
        Protos.TaskInfo completedTemplate = Protos.TaskInfo.newBuilder(template)
                .setName(name)
                .setTaskId(TaskUtils.toTaskId(name))
                .setData(data.getBytes())
                .build();

        completedTemplate = org.apache.mesos.offer.TaskUtils.clearTransient(completedTemplate);

        return new CompactTask(completedTemplate);
    }

    protected CompactTask(final Protos.TaskInfo info) {
        super(info);
    }

    @Override
    public CompactTask update(Protos.Offer offer) {
        return new CompactTask(getBuilder()
            .setSlaveId(offer.getSlaveId())
            .setData(getData().withHostname(offer.getHostname()).getBytes())
            .build());
    }

    @Override
    public CompactTask updateId() {
        return new CompactTask(getBuilder().setTaskId(createId(getName()))
            .build());
    }

    @Override
    public CompactTask update(CassandraTaskStatus status) {
        if (status.getType() == TYPE.COMPACT &&
            getId().equalsIgnoreCase(status.getId())) {
            return update(status.getState());
        }
        return this;
    }

    @Override
    public CompactTask update(Protos.TaskState state) {
        return new CompactTask(getBuilder().setData(
            getData().withState(state).getBytes()).build());
    }

    @Override
    public CompactStatus createStatus(
            Protos.TaskState state,
            Optional<String> message) {

        Protos.TaskStatus.Builder builder = getStatusBuilder();
        if (message.isPresent()) {
            builder.setMessage(message.get());
        }

        return CompactStatus.create(builder
                .setData(CassandraData.createCompactStatusData().getBytes())
                .setState(state)
                .build());
    }


    public CompactContext getCompactContext() {
        return getData().getCompactContext();
    }
}
