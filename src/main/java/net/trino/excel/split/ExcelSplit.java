package net.trino.excel.split;

import static com.google.common.collect.ImmutableList.copyOf;
import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;
import java.util.List;
import java.util.Objects;
import net.trino.excel.handle.ExcelTableHandle;

/**
 * Represents a unit of work for Trino to read data from an Excel source.
 * <p>In Trino's architecture, a {@link ConnectorSplit} represents a "split" of a table's data
 * essentially a portion that can be scanned independently by a worker node. For example, when scanning a large file or
 * multiple files, each split might represent a file, a range of rows, or even a specific sheet.
 * <p>The Trino coordinator assigns splits to workers for parallel execution.
 * This implementation is for an Excel-based connector, where each {@code ExcelSplit} may represent one file, sheet, or
 * logical section of an Excel file. However, since Excel files are typically small and not distributed across a
 * cluster, this implementation assumes the split can be read by any worker.
 *
 * <p>The {@code addresses} field can be used to indicate preferred hosts for reading the split.
 * In distributed file systems or co-located setups, this can optimize performance. For now, it defaults to being
 * accessible from any host.
 *
 * <p> ConnectorSplitSource is responsible for producing data splits for Trino execution. A split represents a unit of
 * parallel work, and the engine repeatedly calls getNextBatch() to retrieve splits in chunks.
 * <p> There are several common types of ConnectorSplitSource used across Trino connectors:
 * <ul>
 *   <li><b>FixedSplitSource</b> – Eager, in-memory source with a fixed list of splits. Suitable for static data.</li>
 *   <li><b>QueueingSplitSource</b> – Supports asynchronous addition of splits. Common in file-based or streaming connectors.</li>
 *   <li><b>Future-based SplitSource</b> – Wraps a future to delay split creation until dynamic filters or metadata are ready.</li>
 *   <li><b>WrappingSplitSource</b> – Decorates another split source to add behavior (e.g., filtering, metrics, logging).</li>
 *   <li><b>CompositeSplitSource</b> – Combines multiple split sources, useful for merging sources from partitions or shards.</li>
 *   <li><b>PartitionedSplitSource</b> – Conceptual pattern where splits are grouped by partitions, buckets, or segments.</li>
 *   <li><b>RemoteSplitSource</b> – Internal Trino engine implementation for distributed execution; not connector-specific.</li>
 * </ul>
 * <p>
 */
public class ExcelSplit implements ConnectorSplit {

    private final ExcelTableHandle tableHandle;
    /**
     * In a non-distributed setup or simple cases, addresses might be empty or localhost. In a distributed setup, this
     * `should` indicate which Trino nodes are preferred to read this data (e.g., if data is co-located with workers).
     * For now, we assume any worker can read any file.
     */
    private final List<HostAddress> addresses;

    @JsonCreator
    public ExcelSplit(
            @JsonProperty("tableHandle") ExcelTableHandle tableHandle,
            @JsonProperty("addresses") List<HostAddress> addresses) {
        this.tableHandle = requireNonNull(tableHandle, "table-handle is null");
        this.addresses = copyOf(requireNonNull(addresses, "addresses is null"));
    }

    @JsonProperty
    public ExcelTableHandle getTableHandle() {
        return tableHandle;
    }

    @Override
    @JsonProperty("addresses")
    public List<HostAddress> getAddresses() {
        return addresses;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ExcelSplit that = (ExcelSplit) o;
        return Objects.equals(tableHandle, that.tableHandle) && Objects.equals(addresses, that.addresses);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableHandle, addresses);
    }

    @Override
    public String toString() {
        return String.format("'excel-split': {'table-handle'='%s', 'addresses'='%s'}", tableHandle, addresses);
    }
}
