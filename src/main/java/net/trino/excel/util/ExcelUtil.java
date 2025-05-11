package net.trino.excel.util;

import io.airlift.log.Logger;
import java.io.IOException;
import java.util.List;
import org.apache.poi.ss.usermodel.Workbook;

/**
 * Utility class providing helper methods for working with Excel files and workbooks. Includes functionality for
 * locating Excel files by schema name and safely closing workbooks.
 */
public class ExcelUtil {

    private static final Logger log = Logger.get(ExcelZipExtractor.class);

    private ExcelUtil() {

    }

    /**
     * Finds the full Excel file name from a list of extracted files that matches the given schema name
     * (case-insensitive match, ignoring file extension).
     *
     * @param allExtractedFileNames list of all filenames extracted from a ZIP archive
     * @param schemaName            the logical schema name (expected file name without extension)
     * @return the full file name including extension if a match is found; {@code null} otherwise
     */
    public static String findFullExcelFileName(List<String> allExtractedFileNames, String schemaName) {
        // schemaName is filename without extension. We need to find the full name.
        for (String fullName : allExtractedFileNames) {
            int dotIndex = fullName.lastIndexOf('.');
            String nameWithoutExt = (dotIndex == -1) ? fullName : fullName.substring(0, dotIndex);
            if (nameWithoutExt.equalsIgnoreCase(schemaName)) {
                return fullName;
            }
        }
        return null;
    }

    /**
     * Safely closes the provided Excel {@link Workbook}, suppressing any {@link IOException} that may occur during the
     * close operation.
     *
     * @param workbook the Excel workbook to close; may be {@code null}
     */
    public static void closeWorkbookQuietly(Workbook workbook) {
        if (workbook != null) {
            try {
                workbook.close();
            } catch (IOException e) {
                log.warn(e, "Error closing workbook quietly");
            }
        }
    }

    /**
     * Safely closes the provided Excel {@link Workbook}, suppressing any {@link IOException} that may occur during the
     * close operation. Logs the name of the associated file for debugging purposes.
     *
     * @param workbook the Excel workbook to close; may be {@code null}
     * @param fileName the name of the file associated with the workbook (used in log messages)
     */
    public static void closeWorkbookQuietly(Workbook workbook, String fileName) {
        if (workbook != null) {
            try {
                workbook.close();
            } catch (IOException e) {
                log.warn(e, "Error closing workbook for file '%s' quietly in ExcelSplitManager", fileName);
            }
        }
    }
}
