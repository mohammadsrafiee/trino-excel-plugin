package net.trino.excel.split;

import static java.util.Objects.requireNonNull;

import io.airlift.log.Logger;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.RecordSet;
import jakarta.inject.Inject;
import java.net.URL;
import java.util.List;
import net.trino.excel.communicator.ExcelErrorCode;
import net.trino.excel.handle.ExcelColumnHandle;
import net.trino.excel.handle.ExcelTableHandle;
import net.trino.excel.plugin.ExcelConfig;
import net.trino.excel.util.ExcelUtil;
import net.trino.excel.util.ExcelZipExtractor;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

/**
 * Provides {@link RecordSet} instances for Excel-based tables by reading data from Excel files contained within a
 * remote ZIP archive. This class is responsible for:
 * <ul>
 *   <li>Extracting Excel files from a ZIP URL</li>
 *   <li>Opening workbooks and locating sheets</li>
 *   <li>Mapping columns to {@link ExcelColumnHandle}</li>
 *   <li>Returning a fully initialized {@link ExcelRecordSet} that can be read by Trino</li>
 * </ul>
 * <p>
 * The lifecycle of file resources (e.g., {@link Workbook}, temporary files) is managed
 * carefully to avoid resource leaks, especially in error paths.
 */
public class ExcelRecordSetProvider implements ConnectorRecordSetProvider {

    private static final Logger log = Logger.get(ExcelRecordSetProvider.class);

    private final URL excelZipUrl;

    /**
     * Constructs a new provider configured to read Excel data from the given ZIP archive URL.
     *
     * @param config the Excel configuration containing the ZIP URL
     */
    @Inject
    public ExcelRecordSetProvider(ExcelConfig config) {
        this.excelZipUrl = requireNonNull(config, "config is null").getExcelZipUrl();
        log.info("ExcelRecordSetProvider configured with ZIP URL: %s", excelZipUrl);
    }

    /**
     * Retrieves a {@link RecordSet} that reads rows from the specified Excel sheet.
     *
     * @param transaction the transaction handle
     * @param session     the session context
     * @param split       the split (contains which Excel file to read)
     * @param table       the table handle
     * @param columns     the list of projected columns
     * @return a {@link RecordSet} that allows Trino to read from the Excel sheet
     * @throws TrinoException if the file or sheet is missing or unreadable
     */
    @Override
    public RecordSet getRecordSet(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<? extends ColumnHandle> columns) {
        ExcelTableHandle excelTable = ((ExcelTableHandle) table);
        String schemaName = ((ExcelTableHandle) table).getSchemaName();
        List<ExcelColumnHandle> columnHandles = columns.stream().map(ExcelColumnHandle.class::cast).toList();

        log.debug("Creating RecordSet for table: %s (from ZIP URL: %s)", excelTable.toSchemaTableName(), excelZipUrl);
        // ExcelZipExtractor is AutoCloseable. The cursor will close it.
        ExcelZipExtractor extractor = new ExcelZipExtractor(excelZipUrl);
        Workbook workbook = null; // Declare workbook here to be accessible in a potential finally block before returning
        boolean extractionSuccessful = false;

        try {
            extractor.downloadAndExtract();
            extractionSuccessful = true; // Mark as successful to allow extractor to be passed on

            String fullExcelFileName = ExcelUtil.findFullExcelFileName(
                    extractor.getExtractedExcelFileNames(), ((ExcelTableHandle) table).getSchemaName());
            if (fullExcelFileName == null) {
                // This specific Excel file (schema) was not in the ZIP
                throw new TrinoException(ExcelErrorCode.EXCEL_FILE_OPEN_ERROR,
                        String.format("Excel file for schema '%s' not found in ZIP from URL: %s",
                                schemaName, excelZipUrl));
            }

            workbook = extractor.openWorkbook(fullExcelFileName);
            Sheet sheet = workbook.getSheet(((ExcelTableHandle) table).getTableName());

            if (sheet == null) {
                // Workbook was opened, but sheet not found. Close workbook before throwing.
                ExcelUtil.closeWorkbookQuietly(workbook);
                throw new TrinoException(ExcelErrorCode.EXCEL_READ_ERROR,
                        String.format("Sheet '%s' not found in Excel file '%s' from ZIP URL: %s",
                                excelTable.getTableName(), fullExcelFileName, excelZipUrl));
            }

            // Pass both workbook and extractor to RecordSet; cursor will close them.
            return new ExcelRecordSet(workbook, sheet, columnHandles, extractor);
        } catch (TrinoException e) {
            // If extraction was successful but later steps failed, the extractor (and its temp files)
            // needs to be cleaned up if we are not passing it to the RecordSet.
            // However, if we are throwing and not returning a RecordSet, the cursor won't close it.
            if (extractionSuccessful) { // Only close extractor if it was successfully initialized and not passed on
                closeExtractorQuietly(extractor); // This path means the extractor won't be passed to cursor
            }
            // If workbook was opened, ensure it is closed on exception before extractor cleanup
            ExcelUtil.closeWorkbookQuietly(workbook);
            throw e;
        } catch (Exception e) {
            if (extractionSuccessful) {
                closeExtractorQuietly(extractor);
            }
            ExcelUtil.closeWorkbookQuietly(workbook);
            throw new TrinoException(ExcelErrorCode.EXCEL_READ_ERROR,
                    String.format(
                            "Error creating RecordSet for table %s from ZIP URL [ %s ]: %s",
                            excelTable.toSchemaTableName(), excelZipUrl, e.getMessage()
                    ), e);
        }
    }

    private void closeExtractorQuietly(ExcelZipExtractor extractor) {
        if (extractor != null) {
            try {
                extractor.close();
            } catch (Exception e) {
                log.warn(e, "Error closing ExcelZipExtractor quietly in RecordSetProvider");
            }
        }
    }
}
