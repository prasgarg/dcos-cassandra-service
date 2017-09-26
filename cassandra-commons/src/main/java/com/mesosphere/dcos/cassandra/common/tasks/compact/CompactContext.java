package com.mesosphere.dcos.cassandra.common.tasks.compact;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mesosphere.dcos.cassandra.common.tasks.ClusterTaskContext;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * CompactContext implements ClusterTaskContext to provide a context for
 * cluster wide compaction.
 */
public class CompactContext implements ClusterTaskContext{

	 /**
     * Creates a new CompactContext.
     *
     * @param nodes          The nodes on which compact will be performed.
     * @param keySpaces      The key spaces that will be compacted. If empty, all
     *                       non-system key spaces will be compacted.
     * @param columnFamilies The column families that will be compacted. If
     *                       empty, all column families for the indicated key
     *                       spaces will be compacted.
     * @return A new CompactContext.
     */
    @JsonCreator
    public static CompactContext create(
        @JsonProperty("nodes") final List<String> nodes,
        @JsonProperty("key_spaces") final List<String> keySpaces,
        @JsonProperty("column_families") final List<String> columnFamilies) {
        return new CompactContext(nodes, keySpaces, columnFamilies);
    }

    @JsonProperty("nodes")
    private final List<String> nodes;
    @JsonProperty("key_spaces")
    private final List<String> keySpaces;
    @JsonProperty("column_families")
    private final List<String> columnFamilies;

    /**
     * Constructs a new CompactContext.
     *
     * @param nodes          The nodes on which compact will be performed.
     * @param keySpaces      The key spaces that will be compacted. If empty, all
     *                       non-system key spaces will be compacted.
     * @param columnFamilies The column families that will be compacted. If
     *                       empty, all column families for the indicated key
     *                       spaces will be compacted.
     */
    public CompactContext(final List<String> nodes,
                         final List<String> keySpaces,
                         final List<String> columnFamilies) {
        this.nodes = (nodes == null) ? Collections.emptyList() : nodes;
        this.keySpaces = (keySpaces == null) ?
            Collections.emptyList() :
            keySpaces;
        this.columnFamilies = (columnFamilies == null) ?
            Collections.emptyList() :
            columnFamilies;
    }

    /**
     * Gets the nodes to compacted.
     *
     * @return The nodes that will be compacted.
     */
    public List<String> getNodes() {
        return nodes;
    }

    /**
     * Gets the column families.
     *
     * @return The column families that will be compacted. If empty, all
     * column families for the indicated key spaces will be compacted.
     */
    public List<String> getColumnFamilies() {
        return columnFamilies;
    }

    /**
     * Gets the key spaces.
     *
     * @return The key spaces that will be compacted. If empty, all non-system
     * key spaces will be compacted.
     */
    public List<String> getKeySpaces() {
        return keySpaces;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CompactContext)) return false;
        CompactContext that = (CompactContext) o;
        return Objects.equals(getNodes(), that.getNodes()) &&
            Objects.equals(getKeySpaces(), that.getKeySpaces()) &&
            Objects.equals(getColumnFamilies(),
                that.getColumnFamilies());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getNodes(), getKeySpaces(), getColumnFamilies());
    }

    @Override
    public String toString() {
        return JsonUtils.toJsonString(this);
    }
}
