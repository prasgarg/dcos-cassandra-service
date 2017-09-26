package com.mesosphere.dcos.cassandra.executor.tasks;

import com.mesosphere.dcos.cassandra.common.tasks.compact.CompactContext;
import com.mesosphere.dcos.cassandra.common.tasks.compact.CompactStatus;
import com.mesosphere.dcos.cassandra.common.tasks.compact.CompactTask;
import com.mesosphere.dcos.cassandra.executor.CassandraDaemonProcess;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CompactTest {
	@Mock
    private ExecutorDriver executorDriver;

    @Mock
    private CassandraDaemonProcess cassandraDaemonProcess;

    @Mock
    private CompactTask compactTask;

    @Mock
    private CompactStatus compactStatus;

    private Compact compact;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        CompactContext compactContext = CompactContext.create(
                Collections.singletonList("node-1"),
                Collections.singletonList("my_keyspace"),
                Arrays.asList("table1", "table2"));
        when(compactTask.getCompactContext()).thenReturn(compactContext);
        when(compactTask.createStatus(any(Protos.TaskState.class), any(Optional.class))).thenReturn(compactStatus);
        compact = new Compact(executorDriver, cassandraDaemonProcess, compactTask);
    }

    @Test
    public void testCompactKeyspace() throws Exception {
        compact.run();

        ArgumentCaptor<List> optionsCaptor = ArgumentCaptor.forClass(List.class);
        verify(cassandraDaemonProcess).compact(eq("my_keyspace"), optionsCaptor.capture());
        List<String> compactColumnFamilies = optionsCaptor.getValue();
        
        assertEquals(compactColumnFamilies.get(0), "table1");
        assertEquals(compactColumnFamilies.get(1), "table2");
        
    }
}
