package net.trino.excel.handle;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

/**
 * Represents a column in an Excel-based Trino connector. This class implements {@link ColumnHandle} and is used by the
 * Trino engine to identify and describe individual columns in an Excel sheet. It contains metadata such as the column's
 * name and its {@link Type}. The class is immutable and supports JSON serialization/deserialization for plan and
 * metadata exchange.
 */
public record ExcelColumnHandle(
        @JsonProperty("columnName") String columnName,
        @JsonProperty("columnType") Type columnType,
        @JsonProperty("ordinalPosition") int ordinalPosition)
        implements ColumnHandle {

    public ExcelColumnHandle {
        requireNonNull(columnName, "column-name is null");
        requireNonNull(columnType, "column-type is null");
    }

    public ColumnMetadata getColumnMetadata() {
        return new ColumnMetadata(columnName, columnType);
    }
}
