package net.trino.excel.handle;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents a handle to a table in the Excel Trino connector. This class links a Trino logical table to a physical
 * location in an Excel file. The `schemaName` maps to the Excel file name (without extension), and the `tableName` maps
 * to the sheet name within that file. It also holds an optional {@link TupleDomain} constraint for predicate push down.
 */
public class ExcelTableHandle implements ConnectorTableHandle {

    /**
     * Corresponds to filename without extension.
     */
    private final String schemaName;
    /**
     * Corresponds to sheet name.
     */
    private final String tableName;

    /**
     * TupleDomain representing column constraints (filters) for potential predicate push down.
     */
    private final TupleDomain<ColumnHandle> constraint;

    /**
     * JSON constructor used for deserialization.
     *
     * @param schemaName the Excel file name (without extension)
     * @param tableName  the sheet name inside the Excel file
     * @param constraint the constraint applied to this table handle (used for predicate push down)
     */
    @JsonCreator
    public ExcelTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint) {
        this.schemaName = requireNonNull(schemaName, "schema-name is null");
        this.tableName = requireNonNull(tableName, "table-name is null");
        this.constraint = requireNonNull(constraint, "constraint is null");
    }

    /**
     * Constructor without constraint for simpler creation, defaults to all()
     *
     * @param schemaName Corresponds to filename without extension.
     * @param tableName  Corresponds to sheet name.
     */
    public ExcelTableHandle(String schemaName, String tableName) {
        this(schemaName, tableName, TupleDomain.all());
    }

    /**
     * Returns the schema name (Excel filename without extension).
     */
    @JsonProperty
    public String getSchemaName() {
        return schemaName;
    }

    /**
     * Returns the table name (sheet name in the Excel file).
     */
    @JsonProperty
    public String getTableName() {
        return tableName;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint() {
        return constraint;
    }

    public SchemaTableName toSchemaTableName() {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ExcelTableHandle that = (ExcelTableHandle) o;
        return Objects.equals(schemaName, that.schemaName) &&
                Objects.equals(tableName, that.tableName) &&
                Objects.equals(constraint, that.constraint);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaName, tableName, constraint);
    }

    @Override
    public String toString() {
        return String.format("%s : %s [ %s ]", schemaName, tableName, constraint);
    }

    public ExcelTableHandle withConstraint(TupleDomain<ColumnHandle> newConstraint) {
        // Return a new instance with the updated constraint
        return new ExcelTableHandle(this.schemaName, this.tableName, newConstraint);
    }

    /**
     * Stub for predicate push down support. This method may be used in the future to apply filters early, potentially
     * improving performance. Currently, no filter is applied at this layer.
     *
     * @param constraint the constraint to attempt to push down
     * @param columns    map of column names to column handles
     * @return an empty result, meaning no filter was applied
     */
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            Constraint constraint,
            Map<String, ColumnHandle> columns) {
        // TODO: Implement predicate push down logic if desired
        //   This involves translating Trino constraints into something the Excel reader can use (if possible)
        //   or simply updating the handle's constraint field.
        //   For now, we don't apply any filtering at this level.
        return Optional.empty();
    }
}
