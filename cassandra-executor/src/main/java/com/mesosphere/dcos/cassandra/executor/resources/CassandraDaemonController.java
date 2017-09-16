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
import static java.lang.String.format;

import com.codahale.metrics.annotation.Counted;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.config.CassandraConfig;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraStatus;
import com.mesosphere.dcos.cassandra.executor.CassandraDaemonProcess;
import com.mesosphere.dcos.cassandra.executor.CassandraExecutor;

import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.nodetool.TableStats;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.mesos.Executor;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.InstanceNotFoundException;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
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
    public Map getHeapUsage() {
        NodeProbe probe = getDaemon().getProbe();
        long secondsUp = probe.getUptime() / 1000;

        // Memory usage
        MemoryUsage heapUsage = probe.getHeapMemoryUsage();
        double memUsed = (double) heapUsage.getUsed() / (1024 * 1024);
        double memMax = (double) heapUsage.getMax() / (1024 * 1024);
        long exception = probe.getStorageMetric("Exceptions");
        String load =  probe.getLoadString();

        Map propertyMap = new HashMap();
        propertyMap.put("secondsUp", secondsUp);
        propertyMap.put("memUsed", memUsed);
        propertyMap.put("memMax", memMax);
        propertyMap.put("exceptions", exception);
        propertyMap.put("load", load);
        return propertyMap;
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

    @GET
    @Counted
    @Path("/cfhistograms")
    public List getLatencyStats(@QueryParam("keyspace") final String  keyspace, @QueryParam("table") final String table) {
        NodeProbe probe = getDaemon().getProbe();
        List<String> result = new LinkedList<>();
        long[] estimatedPartitionSize =
                        (long[]) probe.getColumnFamilyMetric(keyspace, table, "EstimatedPartitionSizeHistogram");
        long[] estimatedColumnCount =
                        (long[]) probe.getColumnFamilyMetric(keyspace, table, "EstimatedColumnCountHistogram");

        // build arrays to store percentile values
        double[] estimatedRowSizePercentiles = new double[7];
        double[] estimatedColumnCountPercentiles = new double[7];
        double[] offsetPercentiles = new double[] {0.5, 0.75, 0.95, 0.98, 0.99};

        if (ArrayUtils.isEmpty(estimatedPartitionSize) || ArrayUtils.isEmpty(estimatedColumnCount)) {
            System.err.println("No SSTables exists, unable to calculate 'Partition Size' and 'Cell Count' percentiles");

            for (int i = 0; i < 7; i++) {
                estimatedRowSizePercentiles[i] = Double.NaN;
                estimatedColumnCountPercentiles[i] = Double.NaN;
            }
        }else {
            EstimatedHistogram partitionSizeHist = new EstimatedHistogram(estimatedPartitionSize);
            EstimatedHistogram columnCountHist = new EstimatedHistogram(estimatedColumnCount);

            if (partitionSizeHist.isOverflowed()) {
                System.err.println(String.format("Row sizes are larger than %s, unable to calculate percentiles",
                                partitionSizeHist.getLargestBucketOffset()));
                for (int i = 0; i < offsetPercentiles.length; i++)
                    estimatedRowSizePercentiles[i] = Double.NaN;
            } else {
                for (int i = 0; i < offsetPercentiles.length; i++)
                    estimatedRowSizePercentiles[i] = partitionSizeHist.percentile(offsetPercentiles[i]);
            }

            if (columnCountHist.isOverflowed()) {
                System.err.println(String.format("Column counts are larger than %s, unable to calculate percentiles",
                                columnCountHist.getLargestBucketOffset()));
                for (int i = 0; i < estimatedColumnCountPercentiles.length; i++)
                    estimatedColumnCountPercentiles[i] = Double.NaN;
            } else {
                for (int i = 0; i < offsetPercentiles.length; i++)
                    estimatedColumnCountPercentiles[i] = columnCountHist.percentile(offsetPercentiles[i]);
            }

            // min value
            estimatedRowSizePercentiles[5] = partitionSizeHist.min();
            estimatedColumnCountPercentiles[5] = columnCountHist.min();
            // max value
            estimatedRowSizePercentiles[6] = partitionSizeHist.max();
            estimatedColumnCountPercentiles[6] = columnCountHist.max();
        }

        String[] percentiles = new String[] {"50%", "75%", "95%", "98%", "99%", "Min", "Max"};
        double[] readLatency = probe.metricPercentilesAsArray((CassandraMetricsRegistry.JmxTimerMBean) probe
                        .getColumnFamilyMetric(keyspace, table, "ReadLatency"));
        double[] writeLatency = probe.metricPercentilesAsArray((CassandraMetricsRegistry.JmxTimerMBean) probe
                        .getColumnFamilyMetric(keyspace, table, "WriteLatency"));
        double[] sstablesPerRead = probe.metricPercentilesAsArray((CassandraMetricsRegistry.JmxHistogramMBean) probe
                        .getColumnFamilyMetric(keyspace, table, "SSTablesPerReadHistogram"));

        result.add(String.format("%s/%s histograms", keyspace, table));
        result.add(String.format("%-10s%10s%18s%18s%18s%18s", "Percentile", "SSTables", "Write Latency", "Read Latency",
                        "Partition Size", "Cell Count"));
        result.add(String.format("%-10s%10s%18s%18s%18s%18s", "", "", "(micros)", "(micros)", "(bytes)", ""));

        for (int i = 0; i < percentiles.length; i++) {
            result.add(String.format("%-10s%10.2f%18.2f%18.2f%18.0f%18.0f", percentiles[i], sstablesPerRead[i],
                            writeLatency[i], readLatency[i], estimatedRowSizePercentiles[i],
                            estimatedColumnCountPercentiles[i]));

        }


        return result;
    }

    private String format(long bytes, boolean humanReadable) {
        return humanReadable ? FileUtils.stringifyFileSize(bytes) : Long.toString(bytes);
    }

    @GET
    @Counted
    @Path("/cfstats")
    public List getcfStats(@QueryParam("keyspace") final String  keyspace,@QueryParam("table") final String tableIp) {
        boolean humanReadable = false;
        NodeProbe probe = getDaemon().getProbe();
        List<String> result = new LinkedList<>();
        Map<String, List<ColumnFamilyStoreMBean>> tableStoreMap = new HashMap<>();
        Iterator<Map.Entry<String, ColumnFamilyStoreMBean>> tables = probe.getColumnFamilyStoreMBeanProxies();

        while (tables.hasNext())
        {
            Map.Entry<String, ColumnFamilyStoreMBean> entry = tables.next();
            String keyspaceName = entry.getKey();
            ColumnFamilyStoreMBean tableProxy = entry.getValue();

            if(!keyspaceName.equalsIgnoreCase(keyspace) || !tableIp.equalsIgnoreCase(tableProxy.getTableName()))
                continue;
            List<ColumnFamilyStoreMBean> columnFamilies = new ArrayList<>();
            columnFamilies.add(tableProxy);
            tableStoreMap.put(keyspaceName, columnFamilies);
        }

        // print out the table statistics
        for (Map.Entry<String, List<ColumnFamilyStoreMBean>> entry : tableStoreMap.entrySet()) {
            String keyspaceName = entry.getKey();
            List<ColumnFamilyStoreMBean> columnFamilies = entry.getValue();
            long keyspaceReadCount = 0;
            long keyspaceWriteCount = 0;
            int keyspacePendingFlushes = 0;
            double keyspaceTotalReadTime = 0.0f;
            double keyspaceTotalWriteTime = 0.0f;

            result.add("Keyspace: " + keyspaceName);
            for (ColumnFamilyStoreMBean table : columnFamilies) {
                String tableName = table.getColumnFamilyName();
                long writeCount = ((CassandraMetricsRegistry.JmxTimerMBean) probe
                                .getColumnFamilyMetric(keyspaceName, tableName, "WriteLatency")).getCount();
                long readCount = ((CassandraMetricsRegistry.JmxTimerMBean) probe
                                .getColumnFamilyMetric(keyspaceName, tableName, "ReadLatency")).getCount();

                if (readCount > 0) {
                    keyspaceReadCount += readCount;
                    keyspaceTotalReadTime +=
                                    (long) probe.getColumnFamilyMetric(keyspaceName, tableName, "ReadTotalLatency");
                }
                if (writeCount > 0) {
                    keyspaceWriteCount += writeCount;
                    keyspaceTotalWriteTime +=
                                    (long) probe.getColumnFamilyMetric(keyspaceName, tableName, "WriteTotalLatency");
                }
                keyspacePendingFlushes += (long) probe.getColumnFamilyMetric(keyspaceName, tableName, "PendingFlushes");
            }

            double keyspaceReadLatency =
                            keyspaceReadCount > 0 ? keyspaceTotalReadTime / keyspaceReadCount / 1000 : Double.NaN;
            double keyspaceWriteLatency =
                            keyspaceWriteCount > 0 ? keyspaceTotalWriteTime / keyspaceWriteCount / 1000 : Double.NaN;

            result.add("\tRead Count: " + keyspaceReadCount);
            result.add("\tRead Latency: " + String.format("%s", keyspaceReadLatency) + " ms.");
            result.add("\tWrite Count: " + keyspaceWriteCount);
            result.add("\tWrite Latency: " + String.format("%s", keyspaceWriteLatency) + " ms.");
            result.add("\tPending Flushes: " + keyspacePendingFlushes);

            // print out column family statistics for this keyspace
            for (ColumnFamilyStoreMBean table : columnFamilies) {
                String tableName = table.getColumnFamilyName();
                if (tableName.contains("."))
                    result.add("\t\tTable (index): " + tableName);
                else
                    result.add("\t\tTable: " + tableName);

                result.add("\t\tSSTable count: " + probe
                                .getColumnFamilyMetric(keyspaceName, tableName, "LiveSSTableCount"));

                int[] leveledSStables = table.getSSTableCountPerLevel();
                if (leveledSStables != null) {
                    System.out.print("\t\tSSTables in each level: [");
                    for (int level = 0; level < leveledSStables.length; level++) {
                        int count = leveledSStables[level];
                        System.out.print(count);
                        long maxCount = 4L; // for L0
                        if (level > 0)
                            maxCount = (long) Math.pow(10, level);
                        //  show max threshold for level when exceeded
                        if (count > maxCount)
                            System.out.print("/" + maxCount);

                        if (level < leveledSStables.length - 1)
                            System.out.print(", ");
                        else
                            result.add("]");
                    }
                }

                Long memtableOffHeapSize = null;
                Long bloomFilterOffHeapSize = null;
                Long indexSummaryOffHeapSize = null;
                Long compressionMetadataOffHeapSize = null;

                Long offHeapSize = null;

                try {
                    memtableOffHeapSize =
                                    (Long) probe.getColumnFamilyMetric(keyspaceName, tableName, "MemtableOffHeapSize");
                    bloomFilterOffHeapSize = (Long) probe.getColumnFamilyMetric(keyspaceName, tableName,
                                    "BloomFilterOffHeapMemoryUsed");
                    indexSummaryOffHeapSize = (Long) probe.getColumnFamilyMetric(keyspaceName, tableName,
                                    "IndexSummaryOffHeapMemoryUsed");
                    compressionMetadataOffHeapSize = (Long) probe.getColumnFamilyMetric(keyspaceName, tableName,
                                    "CompressionMetadataOffHeapMemoryUsed");

                    offHeapSize = memtableOffHeapSize + bloomFilterOffHeapSize + indexSummaryOffHeapSize + compressionMetadataOffHeapSize;
                } catch (RuntimeException e) {
                    // offheap-metrics introduced in 2.1.3 - older versions do not have the appropriate mbeans
                    if (!(e.getCause() instanceof InstanceNotFoundException))
                        throw e;
                }

                result.add("\t\tSpace used (live): " + format(
                                (Long) probe.getColumnFamilyMetric(keyspaceName, tableName, "LiveDiskSpaceUsed"),
                                humanReadable));
                result.add("\t\tSpace used (total): " + format(
                                (Long) probe.getColumnFamilyMetric(keyspaceName, tableName, "TotalDiskSpaceUsed"),
                                humanReadable));
                result.add("\t\tSpace used by snapshots (total): " + format(
                                (Long) probe.getColumnFamilyMetric(keyspaceName, tableName, "SnapshotsSize"),
                                humanReadable));
                if (offHeapSize != null)
                    result.add("\t\tOff heap memory used (total): " + format(offHeapSize, humanReadable));
                result.add("\t\tSSTable Compression Ratio: " + probe
                                .getColumnFamilyMetric(keyspaceName, tableName, "CompressionRatio"));

                Object estimatedPartitionCount =
                                probe.getColumnFamilyMetric(keyspaceName, tableName, "EstimatedPartitionCount");
                if (Long.valueOf(-1L).equals(estimatedPartitionCount)) {
                    estimatedPartitionCount = 0L;
                }
                result.add("\t\tNumber of keys (estimate): " + estimatedPartitionCount);

                result.add("\t\tMemtable cell count: " + probe
                                .getColumnFamilyMetric(keyspaceName, tableName, "MemtableColumnsCount"));
                result.add("\t\tMemtable data size: " + format(
                                (Long) probe.getColumnFamilyMetric(keyspaceName, tableName, "MemtableLiveDataSize"),
                                humanReadable));
                if (memtableOffHeapSize != null)
                    result.add("\t\tMemtable off heap memory used: " + format(memtableOffHeapSize, humanReadable));
                result.add("\t\tMemtable switch count: " + probe
                                .getColumnFamilyMetric(keyspaceName, tableName, "MemtableSwitchCount"));
                result.add("\t\tLocal read count: " + ((CassandraMetricsRegistry.JmxTimerMBean) probe
                                .getColumnFamilyMetric(keyspaceName, tableName, "ReadLatency")).getCount());
                double localReadLatency = ((CassandraMetricsRegistry.JmxTimerMBean) probe
                                .getColumnFamilyMetric(keyspaceName, tableName, "ReadLatency")).getMean() / 1000;
                double localRLatency = localReadLatency > 0 ? localReadLatency : Double.NaN;
                System.out.printf("\t\tLocal read latency: %01.3f ms%n", localRLatency);
                result.add("\t\tLocal write count: " + ((CassandraMetricsRegistry.JmxTimerMBean) probe
                                .getColumnFamilyMetric(keyspaceName, tableName, "WriteLatency")).getCount());
                double localWriteLatency = ((CassandraMetricsRegistry.JmxTimerMBean) probe
                                .getColumnFamilyMetric(keyspaceName, tableName, "WriteLatency")).getMean() / 1000;
                double localWLatency = localWriteLatency > 0 ? localWriteLatency : Double.NaN;
                System.out.printf("\t\tLocal write latency: %01.3f ms%n", localWLatency);
                result.add("\t\tPending flushes: " + probe
                                .getColumnFamilyMetric(keyspaceName, tableName, "PendingFlushes"));
                result.add("\t\tBloom filter false positives: " + probe
                                .getColumnFamilyMetric(keyspaceName, tableName, "BloomFilterFalsePositives"));
                System.out.printf("\t\tBloom filter false ratio: %s%n", String.format("%01.5f",
                                probe.getColumnFamilyMetric(keyspaceName, tableName, "RecentBloomFilterFalseRatio")));
                result.add("\t\tBloom filter space used: " + format(
                                (Long) probe.getColumnFamilyMetric(keyspaceName, tableName, "BloomFilterDiskSpaceUsed"),
                                humanReadable));
                if (bloomFilterOffHeapSize != null)
                    result.add("\t\tBloom filter off heap memory used: " + format(bloomFilterOffHeapSize,
                                    humanReadable));
                if (indexSummaryOffHeapSize != null)
                    result.add("\t\tIndex summary off heap memory used: " + format(indexSummaryOffHeapSize,
                                    humanReadable));
                if (compressionMetadataOffHeapSize != null)
                    result.add("\t\tCompression metadata off heap memory used: " + format(
                                    compressionMetadataOffHeapSize, humanReadable));

                result.add("\t\tCompacted partition minimum bytes: " + format(
                                (Long) probe.getColumnFamilyMetric(keyspaceName, tableName, "MinPartitionSize"),
                                humanReadable));
                result.add("\t\tCompacted partition maximum bytes: " + format(
                                (Long) probe.getColumnFamilyMetric(keyspaceName, tableName, "MaxPartitionSize"),
                                humanReadable));
                result.add("\t\tCompacted partition mean bytes: " + format(
                                (Long) probe.getColumnFamilyMetric(keyspaceName, tableName, "MeanPartitionSize"),
                                humanReadable));
                CassandraMetricsRegistry.JmxHistogramMBean histogram = (CassandraMetricsRegistry.JmxHistogramMBean) probe
                                .getColumnFamilyMetric(keyspaceName, tableName, "LiveScannedHistogram");
                result.add("\t\tAverage live cells per slice (last five minutes): " + histogram.getMean());
                result.add("\t\tMaximum live cells per slice (last five minutes): " + histogram.getMax());
                histogram = (CassandraMetricsRegistry.JmxHistogramMBean) probe
                                .getColumnFamilyMetric(keyspaceName, tableName, "TombstoneScannedHistogram");
                result.add("\t\tAverage tombstones per slice (last five minutes): " + histogram.getMean());
                result.add("\t\tMaximum tombstones per slice (last five minutes): " + histogram.getMax());

                result.add("");
            }
        }

        return result;

    }

    @GET
    @Counted
    @Path("/proxyhistograms")
    public List getProxyHistograms() {
        NodeProbe probe = getDaemon().getProbe();
        List<String> result = new LinkedList<>();
        String[] percentiles = new String[]{"50%", "75%", "95%", "98%", "99%", "Min", "Max"};
        double[] readLatency = probe.metricPercentilesAsArray(probe.getProxyMetric("Read"));
        double[] writeLatency = probe.metricPercentilesAsArray(probe.getProxyMetric("Write"));
        double[] rangeLatency = probe.metricPercentilesAsArray(probe.getProxyMetric("RangeSlice"));

        result.add(String.format("%-10s%18s%18s%18s", "Percentile", "Read Latency", "Write Latency", "Range Latency"));
        result.add(String.format("%-10s%18s%18s%18s", "", "(micros)", "(micros)", "(micros)"));
        for (int i = 0; i < percentiles.length; i++)
        {
            result.add(String.format("%-10s%18.2f%18.2f%18.2f", percentiles[i], readLatency[i], writeLatency[i], rangeLatency[i]));
        }
        return result;
    }

}
