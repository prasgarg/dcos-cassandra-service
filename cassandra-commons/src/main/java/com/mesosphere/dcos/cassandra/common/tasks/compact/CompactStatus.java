package com.mesosphere.dcos.cassandra.common.tasks.compact;

import org.apache.mesos.Protos;

import com.mesosphere.dcos.cassandra.common.tasks.CassandraTaskStatus;

/**
 * CompactStatus extends CassandraTaskStatus to implement the status Object for
 * CompactTask.
 */
public class CompactStatus extends CassandraTaskStatus{

    public static CompactStatus create(final Protos.TaskStatus status) {
        return new CompactStatus(status);
    }

    protected CompactStatus(final Protos.TaskStatus status) {
        super(status);
    }
}
