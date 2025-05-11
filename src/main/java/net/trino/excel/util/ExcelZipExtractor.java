package net.trino.excel.util;

import static java.util.Objects.requireNonNull;

import io.airlift.log.Logger;
import io.trino.spi.TrinoException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import net.trino.excel.communicator.ExcelErrorCode;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

/**
 * Utility class responsible for downloading a ZIP file containing Excel files (.xls, .xlsx) from a specified URL,
 * extracting its contents into a temporary directory, and providing access to those extracted Excel files. This class
 * is intended to be used within the context of a Trino plugin that reads Excel data sources. It manages temporary file
 * handling and ensures proper cleanup using the {@link AutoCloseable} interface.
 */
public class ExcelZipExtractor implements AutoCloseable {

    private static final Logger log = Logger.get(ExcelZipExtractor.class);

    private static final int CONNECT_TIMEOUT_MS = 10000;
    private static final int READ_TIMEOUT_MS = 60000;
    private static final String TEMP_DIR_PREFIX = "trino_excel_zip_";
    private final URL zipFileUrl;
    private final List<String> extractedExcelFileFullNames = new ArrayList<>();
    private Path tempExtractDir;

    /**
     * Constructs a new {@code ExcelZipExtractor} with the given ZIP file URL.
     *
     * @param zipFileUrl the URL pointing to the ZIP archive containing Excel files; must not be null
     * @throws NullPointerException if {@code zipFileUrl} is null
     */
    public ExcelZipExtractor(URL zipFileUrl) {
        this.zipFileUrl = requireNonNull(zipFileUrl, "zip-file-url is null");
    }

    /**
     * Downloads the ZIP file from the configured URL and extracts Excel files (.xls, .xlsx) to a temporary directory.
     *
     * @throws TrinoException if an error occurs during download or extraction.
     */
    public void downloadAndExtract() {
        log.info("Downloading ZIP file from URL: %s", zipFileUrl);
        try {
            tempExtractDir = Files.createTempDirectory(TEMP_DIR_PREFIX + UUID.randomUUID());
            log.debug("Created temporary extraction directory: %s", tempExtractDir);

            URLConnection connection = zipFileUrl.openConnection();
            connection.setConnectTimeout(CONNECT_TIMEOUT_MS);
            connection.setReadTimeout(READ_TIMEOUT_MS);

            try (InputStream zipInputStream = connection.getInputStream();
                    ZipInputStream zis = new ZipInputStream(zipInputStream)) {

                ZipEntry zipEntry = zis.getNextEntry();
                while (zipEntry != null) {
                    // Use zipEntry.getName() which gives the full path as in the ZIP
                    // To avoid issues with paths, just use the base name for storage and matching
                    String entryFullName = new File(zipEntry.getName()).getName();

                    if (!zipEntry.isDirectory()
                            && (entryFullName.toLowerCase().endsWith(".xlsx")
                            || entryFullName.toLowerCase().endsWith(".xls"))) {
                        Path extractedFilePath = tempExtractDir.resolve(entryFullName);
                        log.debug("Extracting Excel file: %s to %s", entryFullName, extractedFilePath);
                        Files.copy(zis, extractedFilePath, StandardCopyOption.REPLACE_EXISTING);
                        extractedExcelFileFullNames.add(entryFullName); // Store the full name
                    }
                    zis.closeEntry();
                    zipEntry = zis.getNextEntry();
                }
            }
            log.info("Successfully downloaded and extracted %d Excel files from %s to %s",
                    extractedExcelFileFullNames.size(), zipFileUrl, tempExtractDir);
        } catch (IOException e) {
            log.error(e, "IOException while downloading or extracting ZIP from URL: %s", zipFileUrl);
            cleanup(); // Attempt to clean up if extraction failed partially
            throw new TrinoException(ExcelErrorCode.EXCEL_FILE_OPEN_ERROR,
                    String.format("Failed to download or extract ZIP file from URL [ %s ]: %s", zipFileUrl,
                            e.getMessage())
                    , e);
        }
    }

    /**
     * Lists the schema names (Excel filenames without extension) extracted from the ZIP. downloadAndExtract() must be
     * called before this.
     *
     * @return A list of schema names.
     */
    public List<String> getExcelSchemaNames() {
        if (tempExtractDir == null) {
            throw new IllegalStateException("ZIP file has not been downloaded and extracted yet.");
        }
        List<String> schemaNames = new ArrayList<>();
        for (String fileName : extractedExcelFileFullNames) {
            int dotIndex = fileName.lastIndexOf('.');
            schemaNames.add((dotIndex == -1) ? fileName : fileName.substring(0, dotIndex));
        }
        return schemaNames;
    }

    /**
     * Lists the full names of Excel files (e.g., "report.xlsx") extracted from the ZIP. downloadAndExtract() must be
     * called before this.
     *
     * @return An unmodifiable list of extracted Excel filenames with extensions.
     */
    public List<String> getExtractedExcelFileNames() {
        if (tempExtractDir == null) {
            throw new IllegalStateException(
                    "ZIP file has not been downloaded and extracted yet. Call downloadAndExtract() first.");
        }
        return Collections.unmodifiableList(extractedExcelFileFullNames);
    }

    /**
     * Opens a specific Excel file from the extracted content as a Workbook. downloadAndExtract() must be called before
     * this.
     *
     * @param excelFileFullName The name of the Excel file (e.g., "data1.xlsx") within the ZIP.
     * @return The opened Workbook.
     * @throws TrinoException if the file is not found or cannot be opened.
     */
    public Workbook openWorkbook(String excelFileFullName) {
        if (tempExtractDir == null) {
            throw new IllegalStateException("ZIP file has not been downloaded and extracted yet.");
        }
        // excelFileFullName should be the name with extension, as stored in extractedExcelFileFullNames
        Path filePath = tempExtractDir.resolve(excelFileFullName);
        if (!Files.exists(filePath) || !extractedExcelFileFullNames.contains(excelFileFullName)) {
            throw new TrinoException(ExcelErrorCode.EXCEL_FILE_OPEN_ERROR,
                    String.format("Excel file '%s' not found in extracted ZIP content from URL: %s", excelFileFullName,
                            zipFileUrl)
            );
        }
        try (InputStream fis = new FileInputStream(filePath.toFile())) {
            return WorkbookFactory.create(fis);
        } catch (Exception e) {
            log.error(e, "Failed to open workbook for file: %s from extracted ZIP", excelFileFullName);
            throw new TrinoException(ExcelErrorCode.EXCEL_FILE_OPEN_ERROR,
                    String.format("Failed to open Excel file '%s' from extracted ZIP: %s", excelFileFullName,
                            e.getMessage())
                    , e);
        }
    }

    /**
     * Cleans up the temporary directory used for extracting files. Should be called when the extractor is no longer
     * needed.
     */
    @Override
    public void close() {
        cleanup();
    }

    /**
     * Internal method that deletes the temporary extraction directory and its contents. Logs warnings if cleanup fails
     * but does not throw.
     */
    private void cleanup() {
        if (tempExtractDir != null) {
            log.debug("Cleaning up temporary extraction directory: %s", tempExtractDir);
            try (var paths = Files.walk(tempExtractDir)) {
                paths.sorted(java.util.Comparator.reverseOrder())
                        .forEach(path -> {
                            try {
                                Files.delete(path);
                            } catch (IOException e) {
                                log.warn(e, "Failed to delete path: %s", path);
                            }
                        });
                log.info("Successfully cleaned up temporary directory: %s", tempExtractDir);
            } catch (IOException e) {
                log.warn(e, "Failed to clean up temporary extraction directory: %s", tempExtractDir);
            }
            tempExtractDir = null;
            extractedExcelFileFullNames.clear();
        }
    }
}
