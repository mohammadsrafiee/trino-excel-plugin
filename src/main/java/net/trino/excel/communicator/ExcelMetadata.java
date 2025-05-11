package net.trino.excel.communicator;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import jakarta.inject.Inject;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;
import net.trino.excel.handle.ExcelColumnHandle;
import net.trino.excel.handle.ExcelTableHandle;
import net.trino.excel.plugin.ExcelConfig;
import net.trino.excel.util.ExcelUtil;
import net.trino.excel.util.ExcelZipExtractor;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

/**
 * {@code ExcelMetadata} is the Trino {@link ConnectorMetadata} implementation for reading metadata from Excel files
 * contained in a remote ZIP archive. It enables schema and table discovery, table metadata inspection, and column
 * handling for Trino queries on Excel data. Each Excel file in the ZIP archive is treated as a schema, and each sheet
 * within an Excel file is treated as a table. The first row of each sheet is assumed to contain column headers. This
 * class handles:
 * <ul>
 *     <li>Listing schemas (Excel filenames without extensions)</li>
 *     <li>Listing tables (sheet names in Excel files)</li>
 *     <li>Providing table metadata (column names inferred from header rows)</li>
 *     <li>Providing column handles and metadata</li>
 * </ul>
 *
 * <b>Limitations:</b>
 * <ul>
 *     <li>The ZIP file is re-downloaded and re-extracted for many operations, which may affect performance.</li>
 *     <li>All columns are treated as {@link io.trino.spi.type.VarcharType}.</li>
 * </ul>
 * <p>
 * ُhis class logs significant actions and errors using the Airlift logger and throws
 * {@link TrinoException} in case of metadata access failures.
 *
 * @see net.trino.excel.util.ExcelZipExtractor
 * @see net.trino.excel.handle.ExcelTableHandle
 * @see net.trino.excel.handle.ExcelColumnHandle
 */
public class ExcelMetadata implements ConnectorMetadata {

    private static final Logger log = Logger.get(ExcelMetadata.class);
    private static final Type DEFAULT_COLUMN_TYPE = VarcharType.VARCHAR;

    private final URL excelZipUrl;

    @Inject
    public ExcelMetadata(ExcelConfig config) {
        this.excelZipUrl = requireNonNull(config, "config is null").getExcelZipUrl();
        log.info("excel-metadata configured with ZIP URL: %s", excelZipUrl);
    }

    /**
     * Lists all schema names (files name without extension) available in the Excel files contained within a ZIP
     * archive. This method downloads and extracts an Excel ZIP file from the configured URL, then retrieves the list of
     * schema names found in the extracted Excel files.
     *
     * @param session the Trino connector session
     * @return a list of schema names found in the Excel files
     * @throws TrinoException if an expected error related to Trino occurs
     * @throws TrinoException with {@code ExcelErrorCode.EXCEL_READ_ERROR} if an unexpected error occurs while
     *                        processing the ZIP archive
     */
    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        try (ExcelZipExtractor extractor = new ExcelZipExtractor(excelZipUrl)) {
            extractor.downloadAndExtract();
            return ImmutableList.copyOf(extractor.getExcelSchemaNames());
        } catch (TrinoException e) {
            log.warn(e, "Could not list schema names from ZIP URL %s", excelZipUrl);
            throw e;
        } catch (Exception e) {
            log.error(e, "Unexpected error listing schema names from ZIP URL %s", excelZipUrl);
            throw new TrinoException(ExcelErrorCode.EXCEL_READ_ERROR,
                    String.format("Error listing schemas from ZIP URL [ %s ]: %s", excelZipUrl, e.getMessage()), e);
        }
    }

    /**
     * Lists all tables available in the Excel data source (all sheets in all Excel files). If a specific schema name is
     * provided, only the tables within that schema (Excel file) are listed. If no schema name is provided, the method
     * returns tables from all available schemas in the ZIP archive.
     *
     * @param session               the Trino connector session
     * @param otpSelectedSchemaName an optional schema name; if empty, tables from all schemas are listed
     * @return a list of {@link SchemaTableName} representing the available tables
     * @throws TrinoException if an error occurs while retrieving schema or table information
     */
    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> otpSelectedSchemaName) {
        if (otpSelectedSchemaName.isEmpty()) {
            // List tables for all schemas (all sheets in all Excel files) (all Excel files in ZIP)
            ImmutableList.Builder<SchemaTableName> allTables = ImmutableList.builder();
            List<String> schemaNames = listSchemaNames(session);
            for (String schemaName : schemaNames) {
                allTables.addAll(listTablesForSchema(schemaName));
            }
            return allTables.build();
        }
        return listTablesForSchema(otpSelectedSchemaName.get());
    }

    private List<SchemaTableName> listTablesForSchema(String schemaName) {
        // We need to find the original filename with extension to open it via extractor
        // This is a bit inefficient as we re-download/extract here.
        // A cached/shared extractor would be better.
        try (ExcelZipExtractor extractor = new ExcelZipExtractor(excelZipUrl)) {
            extractor.downloadAndExtract();
            // Find the full filename that matches the schemaName (which is filename w/o extension)
            String fullExcelFileName = ExcelUtil.findFullExcelFileName(extractor.getExtractedExcelFileNames(),
                    schemaName);
            if (fullExcelFileName == null) {
                log.warn("Schema '%s' (Excel file) not found in ZIP from URL %s during listTables",
                        schemaName, excelZipUrl);
                return List.of();
            }

            Workbook workbook = null;
            try {
                workbook = extractor.openWorkbook(fullExcelFileName);
                ImmutableList.Builder<SchemaTableName> tables = ImmutableList.builder();
                for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
                    String sheetName = workbook.getSheetName(i);
                    tables.add(new SchemaTableName(schemaName, sheetName));
                }
                return tables.build();
            } finally {
                ExcelUtil.closeWorkbookQuietly(workbook);
            }
        } catch (TrinoException e) {
            log.warn(e, "Could not list tables for schema '%s' from ZIP URL %s", schemaName, excelZipUrl);
            throw e;
        } catch (Exception e) {
            log.warn(e, "Unexpected error listing tables for schema '%s' from ZIP URL %s", schemaName, excelZipUrl);
            throw new TrinoException(ExcelErrorCode.EXCEL_READ_ERROR,
                    String.format("Error listing tables for schema '%s' from ZIP URL [%s]: %s",
                            schemaName, excelZipUrl, e.getMessage()), e);
        }
    }

    /**
     * Creates a {@link ConnectorTableHandle} for the specified schema and table name. This method does not validate the
     * existence of the schema or table to avoid triggering an early download or extraction of the Excel ZIP archive.
     * Validation is deferred to {@code getTableMetadata}.
     *
     * @param session      the Trino connector session
     * @param tableName    the schema and table name
     * @param startVersion optional starting version of the table (not used in this implementation)
     * @param endVersion   optional ending version of the table (not used in this implementation)
     * @return a {@link ConnectorTableHandle} representing the requested table
     * @throws NullPointerException if {@code tableName} is null
     */
    @Override
    public ConnectorTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion) {
        requireNonNull(tableName, "table-name is null");
        // We don't validate schema/table existence here to avoid premature download/extraction.
        // Validation will happen in getTableMetadata.
        return new ExcelTableHandle(tableName.getSchemaName(), tableName.getTableName());
    }

    /**
     * Retrieves the metadata for a given table in the Excel-based data source. This method downloads and extracts a ZIP
     * archive containing Excel files, locates the target Excel file corresponding to the schema name, and identifies
     * the sheet representing the table. It reads the first row of the sheet to determine column names and builds column
     * metadata using a default data type for all columns. If the Excel file or sheet does not exist, a
     * {@link TableNotFoundException} is thrown.
     *
     * @param session     the Trino connector session
     * @param tableHandle the handle representing the target table
     * @return metadata describing the table structure, including column names and types
     * @throws TableNotFoundException if the specified schema (Excel file) or sheet (table) is not found
     * @throws TrinoException         with {@code ExcelErrorCode.EXCEL_READ_ERROR} if an error occurs while reading or
     *                                processing the Excel data
     */
    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle) {
        ExcelTableHandle excelTableHandle = (ExcelTableHandle) tableHandle;
        SchemaTableName schemaTableName = excelTableHandle.toSchemaTableName();
        String schemaName = excelTableHandle.getSchemaName(); // This is Excel filename without ext

        try (ExcelZipExtractor extractor = new ExcelZipExtractor(excelZipUrl)) {
            extractor.downloadAndExtract();
            String fullExcelFileName = ExcelUtil.findFullExcelFileName(extractor.getExtractedExcelFileNames(),
                    schemaName);

            if (fullExcelFileName == null) {
                throw new TableNotFoundException(schemaTableName,
                        String.format(
                                "Schema (Excel file) '%s' not found in ZIP from URL: %s", schemaName, excelZipUrl));
            }

            Workbook workbook = null;
            try {
                workbook = extractor.openWorkbook(fullExcelFileName);
                Sheet sheet = workbook.getSheet(excelTableHandle.getTableName());
                if (sheet == null) {
                    throw new TableNotFoundException(schemaTableName,
                            String.format("Sheet '%s' not found in Excel file '%s' from ZIP URL: %s",
                                    excelTableHandle.getTableName(), fullExcelFileName, excelZipUrl));
                }

                Row headerRow = sheet.getRow(0);
                if (headerRow == null) {
                    log.warn("Sheet '%s' in Excel file '%s' (from ZIP URL '%s') has no header row.",
                            excelTableHandle.getTableName(), fullExcelFileName, excelZipUrl);
                    return new ConnectorTableMetadata(schemaTableName, List.of());
                }

                ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
                for (int i = 0; i < headerRow.getLastCellNum(); i++) {
                    Cell cell = headerRow.getCell(i, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL);
                    String columnName = (cell == null || cell.getCellType() != CellType.STRING)
                            ? String.format("COLUMN_%d", i)
                            : cell.getStringCellValue().trim();
                    if (columnName.isEmpty()) {
                        columnName = String.format("COLUMN_%d", i);
                    }
                    // TODO add correct value type according to next row in excel sheet.
                    columns.add(new ColumnMetadata(columnName, DEFAULT_COLUMN_TYPE));
                }
                return new ConnectorTableMetadata(schemaTableName, columns.build());
            } finally {
                ExcelUtil.closeWorkbookQuietly(workbook);
            }
        } catch (TrinoException e) {
            log.warn(e, "Could not get table metadata for '%s' from ZIP URL %s", schemaTableName, excelZipUrl);
            throw e;
        } catch (Exception e) {
            log.warn(e, "Unexpected error getting table metadata for '%s' from ZIP URL %s",
                    schemaTableName, excelZipUrl);
            throw new TrinoException(ExcelErrorCode.EXCEL_READ_ERROR,
                    String.format("Error reading metadata for table '%s' from ZIP URL [%s]: %s",
                            schemaTableName, excelZipUrl, e.getMessage()), e);
        }
    }

    /**
     * Returns a mapping of column names to their corresponding {@link ColumnHandle} objects for a given table in the
     * Excel-based data source. The method retrieves the table metadata using {@link #getTableMetadata}, then creates a
     * {@link ExcelColumnHandle} for each column based on its name, type, and ordinal position.
     *
     * @param session     the Trino connector session
     * @param tableHandle the handle representing the target table
     * @return a map from column names to {@link ColumnHandle} instances
     * @throws TrinoException if an error occurs while retrieving table metadata
     */
    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
        ConnectorTableMetadata tableMetadata = getTableMetadata(session, tableHandle);
        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        IntStream.range(0, tableMetadata.getColumns().size())
                .forEach(i -> {
                    ColumnMetadata column = tableMetadata.getColumns().get(i);
                    columnHandles.put(column.getName(), new ExcelColumnHandle(column.getName(), column.getType(), i));
                });
        return columnHandles.build();
    }

    /**
     * Retrieves the metadata for a specific column in a table. This method casts the provided {@link ColumnHandle} to
     * an {@link ExcelColumnHandle} and delegates the call to its {@code getColumnMetadata()} method.
     *
     * @param session      the Trino connector session
     * @param tableHandle  the handle representing the table containing the column
     * @param columnHandle the handle representing the column
     * @return the {@link ColumnMetadata} describing the column's name, type, and other properties
     * @throws ClassCastException if the {@code columnHandle} is not an instance of {@code ExcelColumnHandle}
     */
    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle,
            ColumnHandle columnHandle) {
        return ((ExcelColumnHandle) columnHandle).getColumnMetadata();
    }
}
