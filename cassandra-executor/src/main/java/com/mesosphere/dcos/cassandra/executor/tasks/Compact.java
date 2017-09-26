package com.mesosphere.dcos.cassandra.executor.tasks;

import com.mesosphere.dcos.cassandra.common.tasks.compact.CompactTask;
import com.mesosphere.dcos.cassandra.executor.CassandraDaemonProcess;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.executor.ExecutorTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Future;

/**
 * @author akshijai
 *
 */
public class Compact implements ExecutorTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(Compact.class);

    private final CassandraDaemonProcess daemon;
    private final ExecutorDriver driver;
    private final CompactTask task;

    private List<String> getKeySpaces() {
        if (task.getCompactContext().getKeySpaces().isEmpty()) {
            return daemon.getNonSystemKeySpaces();
        } else {
            return task.getCompactContext().getKeySpaces();
        }
    }

    private List<String> getColumnFamilies() {
        return task.getCompactContext().getColumnFamilies();
    }

    private void sendStatus(ExecutorDriver driver, Protos.TaskState state, String message) {
        Protos.TaskStatus status = task.createStatus(state, Optional.of(message)).getTaskStatus();
        driver.sendStatusUpdate(status);
    }

    /**
     * Construct a new Compact.
     *
     * @param driver The ExecutorDriver used to send task status.
     * @param daemon The CassandraDaemonProcess used to compact the node.
     * @param task The CompactTask executed by the Compact.
     */
    public Compact(final ExecutorDriver driver, final CassandraDaemonProcess daemon, final CompactTask task) {
        this.driver = driver;
        this.daemon = daemon;
        this.task = task;
    }

    @Override
    public void run() {
        try {

            final List<String> keySpaces = getKeySpaces();
            final List<String> columnFamilies = getColumnFamilies();
            sendStatus(driver, Protos.TaskState.TASK_RUNNING, String.format(
                            "Starting compact: keySpaces = %s, " + "columnFamilies = %s", keySpaces, columnFamilies));

            for (String keyspace : keySpaces) {
                LOGGER.info("Starting compact : keySpace = {}, " + "columnFamilies = {}", keyspace,
                                Arrays.asList(columnFamilies));

                daemon.compact(keyspace, columnFamilies);

                LOGGER.info("Completed compact : keySpace = {}, " + "columnFamilies = {}", keyspace,
                                Arrays.asList(columnFamilies));
            }

            sendStatus(driver, Protos.TaskState.TASK_FINISHED,
                            String.format("Completed compact: keySpaces = %s, " + "columnFamilies = %s", keySpaces,
                                            Arrays.asList(columnFamilies)));
        } catch (IOException e) {
            LOGGER.error("Compact failed Keyspace not valid:", e);
            sendStatus(driver, Protos.TaskState.TASK_FAILED, e.getMessage());
        } catch (IllegalArgumentException e) {
            LOGGER.error("Compact failed Table not part of keyspace:", e);
            sendStatus(driver, Protos.TaskState.TASK_FAILED, e.getMessage());
        } catch (final Throwable t) {
            LOGGER.error("Compact failed", t);
            sendStatus(driver, Protos.TaskState.TASK_FAILED, t.getMessage());
        }
    }

    @Override
    public void stop(Future<?> future) {
        future.cancel(true);
    }
}
