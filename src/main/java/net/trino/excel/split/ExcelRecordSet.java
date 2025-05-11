package net.trino.excel.split;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;
import java.util.List;
import java.util.stream.Collectors;
import net.trino.excel.handle.ExcelColumnHandle;
import net.trino.excel.util.ExcelZipExtractor;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

/**
 * Represents a set of records for a specific Excel sheet (split). This class holds the necessary information to create
 * a {@link RecordCursor} that can iterate over the rows of the designated sheet. It keeps references to the Apache POI
 * {@link Workbook} and {@link Sheet} objects, along with the list of column handles requested by the query.
 */
public class ExcelRecordSet implements RecordSet {

    private final List<ExcelColumnHandle> columnHandles;
    private final ExcelZipExtractor extractor;
    private final List<Type> columnTypes;
    private final Workbook workbook;
    private final Sheet sheet;

    /**
     * Constructs an {@code ExcelRecordSet}.
     *
     * @param workbook      The Apache POI Workbook object containing the sheet.
     * @param sheet         The Apache POI Sheet object representing the table data.
     * @param columnHandles The list of column handles requested by the Trino query.
     * @param extractor     The ExcelZipExtractor instance used to obtain the workbook. It will be closed by the
     *                      cursor.
     */
    public ExcelRecordSet(Workbook workbook, Sheet sheet, List<ExcelColumnHandle> columnHandles,
            ExcelZipExtractor extractor) {
        this.workbook = requireNonNull(workbook, "workbook is null");
        this.sheet = requireNonNull(sheet, "sheet is null");
        this.columnHandles = ImmutableList.copyOf(requireNonNull(columnHandles, "column-handles is null"));
        this.extractor = requireNonNull(extractor, "extractor is null");
        this.columnTypes = this.columnHandles
                .stream()
                .map(ExcelColumnHandle::columnType)
                .collect(Collectors.collectingAndThen(Collectors.toList(), ImmutableList::copyOf));
    }

    /**
     * Returns the Trino {@link Type} for each column in this RecordSet.
     *
     * @return An immutable list of column types.
     */
    @Override
    public List<Type> getColumnTypes() {
        return columnTypes;
    }

    /**
     * Creates a {@link RecordCursor} to iterate over the rows of the Excel sheet.
     *
     * @return A new {@link ExcelRecordCursor} instance.
     */
    @Override
    public RecordCursor cursor() {
        // Pass workbook and extractor to cursor, so it can manage their lifecycles
        return new ExcelRecordCursor(extractor, workbook, sheet, columnHandles);
    }
}
