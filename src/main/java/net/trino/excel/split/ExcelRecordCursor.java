package net.trino.excel.split;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.util.Objects.requireNonNull;
import static net.trino.excel.communicator.ExcelErrorCode.EXCEL_READ_ERROR;
import static net.trino.excel.communicator.ExcelErrorCode.EXCEL_TYPE_MISMATCH;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.Type;
import java.io.IOException;
import java.time.ZoneId;
import java.util.Iterator;
import java.util.List;
import net.trino.excel.communicator.ExcelErrorCode;
import net.trino.excel.handle.ExcelColumnHandle;
import net.trino.excel.util.ExcelZipExtractor;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

/**
 * {@code ExcelRecordCursor} is a Trino-specific implementation of {@link RecordCursor} that reads rows from an Excel
 * {@link Sheet} and maps them to Trino types. It supports a wide range of Excel cell types, including numeric, string,
 * boolean, date, and formula-based cells. It uses Apache POI to access and interpret the Excel content. The first row
 * of the sheet is assumed to be the header and skipped during reading. This class is responsible for:
 * <ul>
 *     <li>Iterating over rows in the sheet (skipping the header row)</li>
 *     <li>Mapping column values to Trino types like BIGINT, DOUBLE, BOOLEAN, VARCHAR, DATE, TIMESTAMP(3)</li>
 *     <li>Handling formula cells using a {@link FormulaEvaluator}</li>
 *     <li>Gracefully reporting data type mismatches and nulls</li>
 *     <li>Estimating the number of bytes read and total read time</li>
 * </ul>
 *
 * <strong>Thread safety:</strong> This class is not thread-safe and should only be used by a single thread at a time.
 *
 * @see ExcelColumnHandle
 * @see Sheet
 * @see Workbook
 */
public class ExcelRecordCursor implements RecordCursor {

    private static final Logger log = Logger.get(ExcelRecordCursor.class);

    private static final String CURSOR_CLOSED_MSG_ERROR = "Cursor is closed";
    private static final String NO_CURRENT_ROW_MSG_ERROR = "No current row";

    private final List<ExcelColumnHandle> columnHandles;
    private final Iterator<Row> rowIterator;
    private final Workbook workbook;
    private final DataFormatter dataFormatter;
    private final FormulaEvaluator formulaEvaluator;
    private final ExcelZipExtractor extractor;
    private final String sheetNameForLogging;

    private Row currentRow;
    private int recordsRead = 0;
    private boolean closed = false;

    public ExcelRecordCursor(ExcelZipExtractor extractor, Workbook workbook, Sheet sheet,
            List<ExcelColumnHandle> columnHandles) {
        requireNonNull(sheet, "sheet is null");
        this.extractor = requireNonNull(extractor, "extractor is null");
        this.workbook = requireNonNull(workbook, "workbook is null");
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        this.sheetNameForLogging = sheet.getSheetName();

        log.info(
                "ExcelRecordCursor: Initializing for sheet: '%s' (hashCode: %s) in workbook (hashCode: %s). Extractor (hashCode: %s). Columns: %s",
                this.sheetNameForLogging,
                sheet.hashCode(),
                this.workbook.hashCode(),
                this.extractor.hashCode(),
                this.columnHandles.stream().map(ExcelColumnHandle::columnName).toList());

        Iterator<Row> originalIterator = sheet.iterator();
        for (int i = 0; i < 1; i++) {
            if (originalIterator.hasNext()) {
                originalIterator.next();
            }
        }
        this.rowIterator = originalIterator;
        this.dataFormatter = new DataFormatter();
        this.formulaEvaluator = this.workbook.getCreationHelper().createFormulaEvaluator();
        log.debug("ExcelRecordCursor: Iterator initialized for sheet '%s'.", this.sheetNameForLogging);
    }

    @Override
    public long getCompletedBytes() {
        return 0L;
    }

    @Override
    public long getReadTimeNanos() {
        return 0;
    }

    @Override
    public Type getType(int field) {
        checkArgument(field >= 0 && field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).columnType();
    }

    @Override
    public boolean advanceNextPosition() {
        if (closed) {
            return false;
        }
        if (!rowIterator.hasNext()) {
            currentRow = null;
            log.debug("ExcelRecordCursor: No more rows in sheet '%s'. Records read: %d",
                    this.sheetNameForLogging, recordsRead);
            return false;
        }
        currentRow = rowIterator.next();
        recordsRead++;
        // Log every N rows or a specific row number if needed for verbose debugging
        if (recordsRead % 100 == 0) {
            log.info("Read %d records from %s", recordsRead, sheetNameForLogging);
        }
        return true;
    }

    @Override
    public boolean getBoolean(int field) {
        checkState(!closed, CURSOR_CLOSED_MSG_ERROR);
        checkState(currentRow != null, NO_CURRENT_ROW_MSG_ERROR);

        Cell cell = getCell(field);
        if (cell == null || cell.getCellType() == CellType.BLANK) {
            throw getCellException(field);
        }

        try {
            return switch (cell.getCellType()) {
                case BOOLEAN -> cell.getBooleanCellValue();
                case STRING -> parseBooleanFromString(cell.getStringCellValue().trim());
                case NUMERIC -> parseBooleanFromNumeric(cell.getNumericCellValue());
                default -> throw new TrinoException(EXCEL_TYPE_MISMATCH,
                        String.format("Cannot get boolean from cell type %s for column %s, value: %s",
                                cell.getCellType(), columnHandles.get(field).columnName(),
                                dataFormatter.formatCellValue(cell)));
            };
        } catch (Exception e) {
            if (e instanceof TrinoException trinoException) {
                throw trinoException;
            }
            throw handleReadError(e, field, "boolean");
        }
    }

    @Override
    public double getDouble(int field) {
        checkState(!closed, CURSOR_CLOSED_MSG_ERROR);
        checkState(currentRow != null, NO_CURRENT_ROW_MSG_ERROR);

        Cell cell = getCell(field);
        if (cell == null || cell.getCellType() == CellType.BLANK) {
            throw new TrinoException(EXCEL_READ_ERROR,
                    String.format("Cannot get double from null or blank cell for column: %s at row %d",
                            columnHandles.get(field).columnName(), currentRow.getRowNum() + 1));
        }

        try {
            return switch (cell.getCellType()) {
                case NUMERIC -> cell.getNumericCellValue();
                case STRING -> Double.parseDouble(cell.getStringCellValue().trim());
                case FORMULA -> getFormulaValue(cell);
                default -> throw new TrinoException(EXCEL_TYPE_MISMATCH,
                        String.format("Cannot get double from cell type: %s for column %s at %s",
                                cell.getCellType(), columnHandles.get(field).columnName(), cell.getAddress()));
            };
        } catch (NumberFormatException e) {
            throw new TrinoException(EXCEL_TYPE_MISMATCH,
                    String.format("Cannot parse double from cell value '%s' for column %s at %s",
                            dataFormatter.formatCellValue(cell), columnHandles.get(field).columnName(),
                            cell.getAddress()), e);
        } catch (Exception e) {
            if (e instanceof TrinoException trinoException) {
                throw trinoException;
            }
            throw handleReadError(e, field, "double");
        }
    }

    @Override
    public long getLong(int field) {
        checkState(!closed, CURSOR_CLOSED_MSG_ERROR);
        checkState(currentRow != null, NO_CURRENT_ROW_MSG_ERROR);

        Cell cell = getCell(field);

        if (cell == null || cell.getCellType() == CellType.BLANK) {
            throw new TrinoException(EXCEL_READ_ERROR,
                    String.format("Cannot get long from null or blank cell for column: %s at row %d",
                            columnHandles.get(field).columnName(), currentRow.getRowNum() + 1));
        }

        try {
            switch (cell.getCellType()) {
                case NUMERIC:
                    return extractLongFromNumeric(cell.getNumericCellValue(), cell);
                case STRING:
                    return Long.parseLong(cell.getStringCellValue().trim());
                case FORMULA:
                    CellType evaluatedType = formulaEvaluator.evaluateFormulaCell(cell);
                    if (evaluatedType == CellType.NUMERIC) {
                        return extractLongFromNumeric(cell.getNumericCellValue(), cell);
                    }
                    if (evaluatedType == CellType.STRING) {
                        return Long.parseLong(cell.getStringCellValue().trim());
                    }
                    throw new TrinoException(EXCEL_TYPE_MISMATCH,
                            String.format(
                                    "Evaluated formula is not numeric or string convertible to long for cell: %s, type: %s",
                                    cell.getAddress(), evaluatedType));
                default:
                    throw new TrinoException(EXCEL_TYPE_MISMATCH,
                            String.format("Cannot get long from cell type: %s for column %s at %s", cell.getCellType(),
                                    columnHandles.get(field).columnName(), cell.getAddress()));
            }
        } catch (NumberFormatException e) {
            throw new TrinoException(EXCEL_TYPE_MISMATCH,
                    String.format("Cannot parse long from cell value '%s' for column %s at %s",
                            dataFormatter.formatCellValue(cell), columnHandles.get(field).columnName(),
                            cell.getAddress()), e);
        } catch (Exception e) {
            if (e instanceof TrinoException trinoException) {
                throw trinoException;
            }
            throw handleReadError(e, field, "long");
        }
    }

    @Override
    public Slice getSlice(int field) {
        checkState(!closed, CURSOR_CLOSED_MSG_ERROR);
        checkState(currentRow != null, NO_CURRENT_ROW_MSG_ERROR);

        if (isNull(field)) {
            return null;
        }
        Cell cell = getCell(field);
        if (cell == null) {
            log.warn(
                    "Cell was null in getSlice after isNull(field) was false for column %s at row %d. This might indicate an ERROR cell.",
                    columnHandles.get(field).columnName(), currentRow.getRowNum() + 1);
            return null;
        }
        try {
            return utf8Slice(dataFormatter.formatCellValue(cell, formulaEvaluator));
        } catch (Exception e) {
            throw handleReadError(e, field, "varchar");
        }
    }

    @Override
    public Object getObject(int field) {
        checkState(!closed, CURSOR_CLOSED_MSG_ERROR);
        if (isNull(field)) {
            return null;
        }
        Type type = getType(field);
        return switch (type.getTypeSignature().getBase()) {
            case "varchar" -> getSlice(field);
            case "bigint", "integer" -> getLong(field);
            case "double" -> getDouble(field);
            case "boolean" -> getBoolean(field);
            case "date" -> getDateFromCell(field);
            case "timestamp" -> getTimestampFromCell(field);
            default ->
                    throw new UnsupportedOperationException(String.format("Unsupported object type: %s for column %s",
                            type, columnHandles.get(field).columnName()));
        };
    }

    @Override
    public boolean isNull(int field) {
        checkState(!closed, CURSOR_CLOSED_MSG_ERROR);
        checkArgument(field >= 0 && field < columnHandles.size(), "Invalid field index");

        if (currentRow == null) {
            return true;
        }
        Cell cell = getCell(field);
        if (cell == null || cell.getCellType() == CellType.BLANK) {
            return true;
        }
        if (cell.getCellType() == CellType.FORMULA) {
            return formulaEvaluator.evaluateFormulaCell(cell) == CellType.ERROR;
        }
        return false;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        log.info("ExcelRecordCursor: Closing cursor for sheet '%s'. Records read: %d", this.sheetNameForLogging,
                recordsRead);
        try {
            if (workbook != null) {
                log.debug("Closing workbook for sheet: %s", this.sheetNameForLogging);
                workbook.close();
            }
        } catch (IOException e) {
            log.warn(e, "Error closing workbook for sheet: %s", this.sheetNameForLogging);
        } finally {
            if (extractor != null) {
                log.debug("Closing ExcelZipExtractor for sheet: %s.", this.sheetNameForLogging);
                try {
                    extractor.close();
                } catch (Exception e) {
                    log.warn(e, "Error closing ExcelZipExtractor for sheet: %s", this.sheetNameForLogging);
                }
            }
        }
    }

    private boolean parseBooleanFromString(String val) {
        if ("true".equalsIgnoreCase(val)) {
            return true;
        }
        if ("false".equalsIgnoreCase(val)) {
            return false;
        }
        throw new TrinoException(EXCEL_TYPE_MISMATCH, String.format("Invalid boolean string value: %s", val));
    }

    private boolean parseBooleanFromNumeric(double numVal) {
        if (numVal == 1.0 || numVal == 0.0) {
            return numVal == 1.0;
        }
        throw new TrinoException(EXCEL_TYPE_MISMATCH, String.format("Invalid numeric value for boolean: %f", numVal));
    }

    private TrinoException getCellException(int field) {
        if (getType(field).equals(BOOLEAN)) {
            return new TrinoException(EXCEL_READ_ERROR,
                    String.format("Cannot get boolean from null or blank cell. for column: %s at row %d",
                            columnHandles.get(field).columnName(), currentRow.getRowNum() + 1));
        }
        return new TrinoException(EXCEL_READ_ERROR, "Cannot get boolean from null or blank cell.");
    }

    private long extractLongFromNumeric(double value, Cell cell) {
        if (value == Math.floor(value) && !Double.isInfinite(value)) {
            return (long) value;
        }
        throw new TrinoException(EXCEL_TYPE_MISMATCH,
                String.format("Numeric cell value '%s' is not a whole number at %s", value, cell.getAddress()));
    }

    private double getFormulaValue(Cell cell) {
        CellType evaluatedType = formulaEvaluator.evaluateFormulaCell(cell);
        if (evaluatedType == CellType.NUMERIC) {
            return cell.getNumericCellValue();
        }
        if (evaluatedType == CellType.STRING) {
            return Double.parseDouble(cell.getStringCellValue().trim());
        }
        throw new TrinoException(EXCEL_TYPE_MISMATCH,
                String.format("Evaluated formula is not numeric or string convertible to double for cell: %s, type: %s",
                        cell.getAddress(), evaluatedType));
    }

    private int getDateFromCell(int field) {
        Cell cell = getCell(field);
        if (cell == null || cell.getCellType() == CellType.BLANK) {
            throw new TrinoException(EXCEL_READ_ERROR,
                    String.format(
                            "Cannot get Date from null or blank cell for column: %s",
                            columnHandles.get(field).columnName()));
        }
        try {
            if (cell.getCellType() == CellType.NUMERIC && DateUtil.isCellDateFormatted(cell)) {
                return (int) cell.getLocalDateTimeCellValue().toLocalDate().toEpochDay();
            }
            if (cell.getCellType() == CellType.FORMULA && DateUtil.isCellDateFormatted(cell)) {
                return (int) cell.getLocalDateTimeCellValue().toLocalDate().toEpochDay();
            }
            throw new TrinoException(ExcelErrorCode.EXCEL_TYPE_MISMATCH,
                    String.format("Cannot get Date from cell type %s for column %s, value: %s", cell.getCellType(),
                            columnHandles.get(field).columnName(), dataFormatter.formatCellValue(cell)));
        } catch (Exception e) {
            if (e instanceof TrinoException trinoException) {
                throw trinoException;
            }
            throw handleReadError(e, field, "date");
        }
    }

    private long getTimestampFromCell(int field) {
        Cell cell = getCell(field);
        if (cell == null || cell.getCellType() == CellType.BLANK) {
            throw new TrinoException(EXCEL_READ_ERROR,
                    String.format("Cannot get Timestamp from null or blank cell for column: %s",
                            columnHandles.get(field).columnName()));
        }
        try {
            if (cell.getCellType() == CellType.NUMERIC && DateUtil.isCellDateFormatted(cell)) {
                return cell.getLocalDateTimeCellValue().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
            }
            if (cell.getCellType() == CellType.FORMULA && DateUtil.isCellDateFormatted(cell)) {
                return cell.getLocalDateTimeCellValue().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
            }
            throw new TrinoException(ExcelErrorCode.EXCEL_TYPE_MISMATCH,
                    String.format("Cannot get Timestamp from cell type %s for column %s, value: %s", cell.getCellType(),
                            columnHandles.get(field).columnName(), dataFormatter.formatCellValue(cell)));
        } catch (Exception e) {
            if (e instanceof TrinoException trinoException) {
                throw trinoException;
            }
            throw handleReadError(e, field, "timestamp(3)");
        }
    }

    private Cell getCell(int field) {
        ExcelColumnHandle columnHandle = columnHandles.get(field);
        int columnIndex = columnHandle.ordinalPosition();
        if (currentRow == null) {
            log.warn(
                    "ExcelRecordCursor: currentRow is null when trying to getCell for field %d (column index %d) on sheet '%s'.",
                    field, columnIndex, this.sheetNameForLogging);
            return null;
        }
        return currentRow.getCell(columnIndex, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL);
    }

    private RuntimeException handleReadError(Exception e, int field, String targetType) {
        ExcelColumnHandle column = columnHandles.get(field);
        int rowNum = currentRow != null ? currentRow.getRowNum() + 1 : -1;

        String cellAddress = "<unknown_address>";
        String cellValueForError = "<ERROR_READING_CELL_VALUE>";
        Cell problematicCell = null;
        if (currentRow != null) {
            problematicCell = currentRow.getCell(column.ordinalPosition(), Row.MissingCellPolicy.RETURN_BLANK_AS_NULL);
            if (problematicCell != null) {
                cellAddress = problematicCell.getAddress() != null ? problematicCell.getAddress().formatAsString()
                        : String.format("R%dC%d", rowNum, column.ordinalPosition() + 1);
                try {
                    cellValueForError = dataFormatter.formatCellValue(problematicCell, formulaEvaluator);
                } catch (Exception cellReadEx) {
                    log.warn(cellReadEx, "Error trying to read cell value for error reporting at %s.", cellAddress);
                }
            } else {
                cellValueForError = "<NULL_OR_BLANK_CELL>";
            }
        }

        return new TrinoException(
                EXCEL_READ_ERROR,
                String.format(
                        "Error reading value for column '%s' (index %d) at sheet '%s' cell %s (row %d), expected type %s. Cell raw value: '%s'. Error: %s",
                        column.columnName(),
                        column.ordinalPosition(),
                        this.sheetNameForLogging,
                        cellAddress,
                        rowNum,
                        targetType,
                        cellValueForError,
                        e.getMessage()), e);
    }
}
