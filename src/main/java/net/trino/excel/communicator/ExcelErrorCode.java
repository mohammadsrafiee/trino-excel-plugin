package net.trino.excel.communicator;

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;

/**
 * Example error codes for the Excel connector
 */
public enum ExcelErrorCode implements ErrorCodeSupplier {
    /**
     * Indicates an error occurred while reading data from an Excel file
     */
    EXCEL_READ_ERROR(0, ErrorType.EXTERNAL),
    /**
     * Indicates an error occurred trying to open or access an Excel file
     */
    EXCEL_FILE_OPEN_ERROR(1, ErrorType.EXTERNAL),
    /**
     * Indicates an unexpected internal error within the connector
     */
    EXCEL_INTERNAL_ERROR(2, ErrorType.INTERNAL_ERROR),
    /**
     * Indicates a mismatch between expected and actual data type during read
     */
    EXCEL_TYPE_MISMATCH(4, ErrorType.EXTERNAL);

    private final ErrorCode errorCode;

    ExcelErrorCode(int code, ErrorType type) {
        errorCode = new ErrorCode(code, name(), type);
    }

    @Override
    public ErrorCode toErrorCode() {
        return errorCode;
    }
}
