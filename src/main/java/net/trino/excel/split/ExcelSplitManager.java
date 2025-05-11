package net.trino.excel.split;

import static java.util.Objects.requireNonNull;

import io.airlift.log.Logger;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import jakarta.inject.Inject;
import java.net.URL;
import java.util.List;
import net.trino.excel.communicator.ExcelErrorCode;
import net.trino.excel.handle.ExcelTableHandle;
import net.trino.excel.plugin.ExcelConfig;
import net.trino.excel.util.ExcelUtil;
import net.trino.excel.util.ExcelZipExtractor;
import org.apache.poi.ss.usermodel.Workbook;

/**
 * Manages the creation of {@link ConnectorSplit} objects for the Excel connector. This manager downloads a ZIP file
 * specified by a URL, extracts all Excel files (.xls, .xlsx) from it, and then creates a separate {@link ExcelSplit}
 * for each sheet within each of those Excel files. Each Excel file effectively acts as a schema, and each sheet within
 * it as a table.
 */
public class ExcelSplitManager implements ConnectorSplitManager {

    private static final Logger log = Logger.get(ExcelSplitManager.class);
    private static final String ERROR_MESSAGE = "TrinoException during split generation from ZIP URL %s. No splits will be generated.";

    private final URL excelZipUrl;

    /**
     * Constructs an {@code ExcelSplitManager}. This constructor is used by Guice for dependency injection.
     *
     * @param config The Excel connector configuration, used to get the ZIP file URL.
     */
    @Inject
    public ExcelSplitManager(ExcelConfig config) {
        this.excelZipUrl = requireNonNull(config, "config is null").getExcelZipUrl();
        log.info("excel-split-manager configured with ZIP URL: %s", excelZipUrl);
    }

    /**
     * Generates splits for the given table handle.
     * <p> Note: In this implementation, the {@code tableHandle}, {@code dynamicFilter}, and {@code constraint}
     * parameters are effectively ignored during split generation because the manager processes the entire ZIP file to
     * discover all possible tables (sheets) and creates splits for them. The Trino engine will later filter these
     * splits based on the actual table being queried.
     *
     * @param transaction   The current transaction handle.
     * @param session       The current connector session.
     * @param table         The handle for the table for which splits are being requested. This is used by Trino to
     *                      identify the target, but this manager will generate splits for all tables found in the ZIP
     *                      and let Trino filter later.
     * @param dynamicFilter A dynamic filter that might be applied.
     * @param constraint    A constraint that might be applied (e.g., from a WHERE clause).
     * @return A {@link ConnectorSplitSource} containing one {@link ExcelSplit} for each sheet in each Excel file within
     * the ZIP.
     */
    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint) {

        // The ExcelZipExtractor will download and extract files to a temporary directory.
        // It needs to be closed to clean up that directory.
        try (ExcelZipExtractor extractor = new ExcelZipExtractor(excelZipUrl)) {
            extractor.downloadAndExtract();
            List<String> schemaNames = extractor.getExcelSchemaNames();
            List<String> fullExcelFileNames = extractor.getExtractedExcelFileNames(); // Filenames with file extension
            log.info("Found %d Excel files (schemas) in ZIP: %s", schemaNames.size(), schemaNames);
            String schemaName = ((ExcelTableHandle) table).getSchemaName();
            String fullExcelFileName = ExcelUtil.findFullExcelFileName(fullExcelFileNames, schemaName);
            if (fullExcelFileName == null) {
                log.warn("Could not find full filename for schema '%s' in extracted list. Skipping.", schemaName);
            } else {
                ConnectorSplit split = extracted(schemaName, fullExcelFileName, extractor,
                        ((ExcelTableHandle) table).getTableName());
                log.info("Generated split from ZIP URL: %s", excelZipUrl);
                if (split == null) {
                    log.error(ERROR_MESSAGE, excelZipUrl);
                    // If ZIP download/extraction fails, throw to halt processing for this query path.
                    throw new TrinoException(ExcelErrorCode.EXCEL_INTERNAL_ERROR,
                            String.format(ERROR_MESSAGE, excelZipUrl));
                }
                // The ConnectorSplitSource interface provides methods that allow Trino to fetch batches of splits from a data source.
                // These splits are then distributed across Trino's worker nodes for parallel query execution.
                // The interface ensures that data retrieval is efficient and scalable, especially when dealing with large datasets.
                // FixedSplitSource is useful when the list of splits is static and fully determined during split generation.
                // It holds all the ConnectorSplit instances and serves them in batches.
                return new FixedSplitSource(List.of(split));
            }
            // Extractor is closed automatically by try-with-resources, cleaning up temp dir.
        } catch (TrinoException e) {
            log.error(e, ERROR_MESSAGE, excelZipUrl);
            // If ZIP download/extraction fails, throw to halt processing for this query path.
            throw e;
        } catch (Exception e) {
            log.error(e,
                    "Unexpected critical error during split generation from ZIP URL %s. No splits will be generated.",
                    excelZipUrl);
            throw new TrinoException(ExcelErrorCode.EXCEL_INTERNAL_ERROR,
                    String.format(
                            "Critical error during split generation from ZIP URL [ %s ]: %s",
                            excelZipUrl, e.getMessage()),
                    e);
        }
        log.error(ERROR_MESSAGE, excelZipUrl);
        // If ZIP download/extraction fails, throw to halt processing for this query path.
        throw new TrinoException(ExcelErrorCode.EXCEL_INTERNAL_ERROR, String.format(ERROR_MESSAGE, excelZipUrl));
    }

    private ConnectorSplit extracted(
            String schemaName, String fullExcelFileName, ExcelZipExtractor extractor, String tableName) {
        Workbook workbook = null;
        try {
            log.debug("Processing Excel file (schema): %s (full name: %s)", schemaName, fullExcelFileName);
            workbook = extractor.openWorkbook(fullExcelFileName);
            log.debug("Created split for table: %s.%s", schemaName, tableName);
            return new ExcelSplit(new ExcelTableHandle(schemaName, tableName), List.of());
        } catch (TrinoException e) {
            log.warn(e,
                    "TrinoException while processing sheets for Excel file '%s' from ZIP. Some splits might be missing.",
                    fullExcelFileName);
            // Potentially continue to next file if one Excel file is problematic
        } catch (Exception e) {
            log.error(e,
                    "Unexpected error processing sheets for Excel file '%s' from ZIP. Some splits might be missing.",
                    fullExcelFileName);
            // Potentially continue
        } finally {
            ExcelUtil.closeWorkbookQuietly(workbook, fullExcelFileName);
        }
        return null;
    }
}
