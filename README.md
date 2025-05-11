# Trino Excel Connector
The Trino Excel Connector allows Trino to query data from Microsoft Excel files (`.xls` and `.xlsx`) that are packaged within a ZIP archive accessible via a URL. Each Excel file within the ZIP archive is treated as a schema in Trino. Each sheet within an Excel file is treated as a table. The connector assumes the first row of each sheet contains the column headers.

## Features

*   Reads both `.xls` (Excel 97-2003) and `.xlsx` (Excel 2007 and later) file formats.
*   Treats Excel filenames (without extension) as schemas.
*   Treats sheet names as tables.
*   Infers column names from the first row of each sheet.
*   Currently, all data is read as `VARCHAR`.

## Prerequisites

*   Java 17 or later
*   Gradle 7.x or later

## Building the Plugin

To build the plugin, run the following Gradle command from the project root:

```bash
./gradlew clean shadowJar
```

This will produce a fat JAR (e.g., `trino-excel-plugin-*.jar`) in the `build/libs/` directory. This JAR contains the plugin and all its necessary dependencies.

## Installation

1.  **Copy the Plugin JAR**:
    Copy the generated fat JAR (e.g., `build/libs/trino-excel-plugin-*.jar`) to the Trino plugin directory on your Trino coordinator and workers. Create an `excel` subdirectory if it doesn't exist:
    ```bash
    sudo mkdir -p <trino_install_dir>/plugin/excel
    sudo cp build/libs/trino-excel-plugin-*.jar <trino_install_dir>/plugin/excel/
    ```

2.  **Create Catalog Properties File**:
    Create a catalog properties file named `excel.properties` in your Trino configuration directory (e.g., `etc/catalog/excel.properties`).

## Configuration

The `excel.properties` file configures the Excel connector. At a minimum, it requires the connector name and the URL of the ZIP file containing the Excel files.

**Example `etc/catalog/excel.properties`:**

```properties
connector.name=excel
excel.zip-url=http://example.com/path/to/your/excel_files.zip
```

**Configuration Properties:**

*   `connector.name`: Must be `excel`.
*   `excel.zip-url`: The URL pointing to the ZIP archive containing the Excel files. This URL must be accessible by the Trino coordinator and workers.

## Usage

Once the plugin is installed and configured, you can query the Excel data using Trino.

**List Schemas (Excel Files):**
Each Excel filename (without the `.xls` or `.xlsx` extension) within the ZIP archive becomes a schema.

```sql
SHOW SCHEMAS FROM excel;
```
Expected output might be:
```
   Schema
-----------------
 sales_data_2023
 employee_records
 (1 row)
```
(Assuming `sales_data_2023.xlsx` and `employee_records.xls` were in your ZIP file)

**List Tables (Sheets in an Excel File):**
Specify the schema (Excel filename) to list its tables (sheets).

```sql
SHOW TABLES FROM excel."sales_data_2023";
```
Expected output might be:
```
  Table
---------
 Q1_Sales
 Q2_Sales
 Products
(1 row)
```
(Assuming `sales_data_2023.xlsx` contained sheets named `Q1_Sales`, `Q2_Sales`, and `Products`)

**Query Data from a Table (Sheet):**

```sql
SELECT * FROM excel."sales_data_2023"."Q1_Sales";

SELECT "Product Name", "Revenue"
FROM excel."sales_data_2023"."Products"
WHERE "Category" = 'Electronics';
```

**Note on Quoting:** Since schema names (Excel filenames) and table names (sheet names) can contain spaces or special characters, it's a good practice to always quote them in your SQL queries.

## Development & Testing

### Integration Tests

The project includes integration tests using Testcontainers. To run them:

```bash
./gradlew test
```

Ensure you have Docker installed and running, as the tests will spin up Trino and an Nginx container to serve test data. The test ZIP file (`excel_files.zip`) should be placed in `src/test/resources/test-data/`.

## Limitations

*   **Data Types**: Currently, all data from Excel cells is treated as `VARCHAR`. Future enhancements could include type inference.
*   **Performance**: For every metadata operation or query, the ZIP file might be re-downloaded and extracted. This is not optimal for very large ZIP files or frequent access. Caching strategies could improve this.
*   **Error Handling**: While basic error handling is in place, complex or malformed Excel files might lead to unexpected behavior.
*   **Write Operations**: This connector is read-only. It does not support `INSERT`, `UPDATE`, `DELETE`, or `CREATE TABLE`.

## Contributing

Contributions are welcome! Please feel free to submit pull requests or open issues. 
