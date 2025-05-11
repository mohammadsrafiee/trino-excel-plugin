# Guide: Writing a Trino Connector (Illustrated by the Excel Connector)

This guide provides an overview of how to develop a custom connector for Trino, using the concepts and components from
the Excel connector project we've been working on as practical examples.

1. __Introduction to Trino Connectors__
   Trino connectors are plugins that allow Trino to interact with various data sources. A connector acts as a bridge,
   translating Trino's query operations and data type system into the specific API and data model of the target data
   source.
2. __Core SPI Interfaces to Implement__
   The Trino SPI (Service Provider Interface) defines a set of Java interfaces that your connector must implement. Here
   are the most crucial ones:

    * __io.trino.spi.Plugin__:
        * __Purpose__: The entry point for your plugin. Trino discovers plugins using Java's ServiceLoader mechanism.
          This class tells Trino what ConnectorFactory (or other services like custom types or functions) your plugin
          provides.
        * __Excel Plugin Example__: `net.trino.excel.plugin.ExcelPlugin`
      ```java
        public class ExcelPlugin implements Plugin {
          @Override
          public Iterable<ConnectorFactory> getConnectorFactories() {
              return ImmutableList.of(new ExcelConnectorFactory());
          }
        }
      ```

    * __io.trino.spi.connector.ConnectorFactory__:
        * __Purpose__: Responsible for creating instances of your Connector. It receives configuration from Trino
          (catalog properties file) and a ConnectorContext.
        * __Excel Plugin Example__: `net.trino.excel.ExcelConnectorFactory`
      ```java
         public class ExcelConnectorFactory implements ConnectorFactory {
             @Override
             public String getName() {
                 return "excel"; // Unique name for this connector
             }
     
             @Override
             public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
                 Bootstrap app = new Bootstrap(
                     new ExcelModule(), // Your Guice module
                     binder -> { // Bind objects from ConnectorContext if needed
                         binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                         binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                     }
                 );
                 Injector injector = app
                     .doNotInitializeLogging() // Trino server handles logging
                     .setRequiredConfigurationProperties(config) // Passes config from excel.properties
                     .initialize();
                 return injector.getInstance(ExcelConnector.class);
             }
         }
      ```

    * __io.trino.spi.connector.Connector__:
        * __Purpose__: The main object representing an instantiated catalog. It provides access to metadata, data
          splitting, and data reading capabilities.
        * __Excel Plugin Example__: `net.trino.excel.ExcelConnector`

            ```java
            public class ExcelConnector implements Connector {
                private final ConnectorMetadata metadata;
                private final ConnectorSplitManager splitManager;
                private final ConnectorRecordSetProvider recordSetProvider;

                @Inject
                public ExcelConnector(
                        ExcelMetadata metadata, // Guice injects implementations
                        ExcelSplitManager splitManager,
                        ExcelRecordSetProvider recordSetProvider) {
                    this.metadata = metadata;
                    this.splitManager = splitManager;
                    this.recordSetProvider = recordSetProvider;
                }

                @Override
                public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle) {
                    return metadata;
                }

                @Override
                public ConnectorSplitManager getSplitManager() {
                    return splitManager;
                }

                @Override
                public ConnectorRecordSetProvider getRecordSetProvider() {
                    return recordSetProvider;
                }
                // Other methods like getPageSourceProvider (if using Page API), shutdown, etc.
            }
            ```

    * __io.trino.spi.connector.ConnectorMetadata__:
        * __Purpose__ : Handles all metadata operations; `listing schemas`, `listing tables`, `resolving table handles`,
          `providing table metadata` (column names, types), `describing columns`.
        * __Excel Plugin Example__: `net.trino.excel.communicator.ExcelMetadata`
          Implements methods
          like `listSchemaNames`, `listTables`, `getTableHandle`, `getTableMetadata`, `getColumnHandles`, `getColumnMetadata`.
          In the Excel plugin, it interacts with `ExcelZipExtractor` to get information about Excel files (schemas) and
          sheets (tables).

    * __io.trino.spi.connector.ConnectorSplitManager__:
        * __Purpose__: Given a table handle and constraints (like filters from a `WHERE` clause), it divides the table's
          data into `splits`. Each split represents a portion of data that can be processed independently by a worker
          node.
        * __Excel Plugin Example__: `net.trino.excel.split.ExcelSplitManager`
          Implements getSplits.
          For the Excel plugin, one split is typically created per sheet (`table`), as Excel sheets are usually read
          sequentially. The ExcelSplit object contains information needed to locate and read that specific sheet from
          the correct Excel file within the ZIP.

    * __io.trino.spi.connector.ConnectorRecordSetProvider__:
        * __Purpose__: Creates a RecordSet from a given `ConnectorSplit` and a list of `ColumnHandles` (columns to be
          read).
        * __Excel Plugin Example__: `net.trino.excel.communicator.ExcelRecordSetProvider`
          Implements getRecordSet.
          It uses the information in the `ExcelSplit` (schema/file name, table/sheet name, ZIP URL)
          and `ExcelColumnHandles`
          to prepare for data reading. It often instantiates or uses a utility (like `ExcelZipExtractor`) to access the
          raw data and passes it to a `RecordSet`.

    * __io.trino.spi.connector.RecordSet__:
        * __Purpose__: Represents a collection of rows to be processed for a split. It provides the column types and
          creates a `RecordCursor`.
        * __Excel Plugin Example__: `net.trino.excel.communicator.ExcelRecordSet`

        ```java
            public class ExcelRecordSet implements RecordSet {
                private final List<Type> columnTypes;
                private final RecordCursor cursor;

                public ExcelRecordSet(RecordCursor cursor, List<Type> columnTypes) {
                    this.cursor = cursor;
                    this.columnTypes = ImmutableList.copyOf(requireNonNull(columnTypes, "columnTypes is null"));
                }

                @Override
                public List<Type> getColumnTypes() {
                    return columnTypes;
                }

                @Override
                public RecordCursor cursor() {
                    return cursor;
                }
            }
        ```

    * __io.trino.spi.connector.RecordCursor__:
        * __Purpose__: Iterates over the rows of a split, providing data for each column in each row. This is where the
          actual data extraction from the source happens.
        * __Excel Plugin Example__: `net.trino.excel.communicator.ExcelRecordCursor`
          Implements methods like advanceNextPosition (to move to the next
          row), `isNull`, `getBoolean`, `getLong`, `getDouble`,
          `getSlice` (for VARCHAR, VARBINARY, etc.), `getType`, `close`.
          The Excel plugin's cursor reads rows from an Apache POI Sheet object, converting cell values to appropriate
          Trino types (currently all to VARCHAR via Slices.utf8Slice(...)). It also handles resource cleanup (closing
          workbooks, extractors).
3. __Plugin Packaging and Deployment__
    * __Directory Structure__: Trino expects plugins in a specific directory structure within its plugin installation:
      `<trino_plugin_dir>/<connector_name>/your-plugin.jar` (and any other library JARs).
    * __SPI Dependencies__: Your `build.gradle.kts` must include compileOnly(`"io.trino:trino-spi:<version>"`).
      The SPI is provided by Trino at runtime.
    * __Fat JAR (Shadow JAR)__: For most other dependencies (like Apache POI for Excel, HTTP clients),
      you'll bundle them into your plugin JAR using Gradle's
      Shadow `plugin (id("com.github.johnrengelman.shadow") version` "...").
      This creates an `fat JAR.`
        * Make sure to configure the shadowJar task to relocate or exclude dependencies that might conflict with Trino's
          own (e.g., `Guava`, `Jackson`),
          though often for simple plugins, careful dependency selection is key. The Excel plugin includes POI and its
          dependencies.
    * __ServiceLoader Registration__: Create a file named `io.trino.spi.Plugin` in
      the `src/main/resources/META-INF/services/` directory. This file should contain the fully qualified name of your
      Plugin implementation class.
    * __Example for Excel Plugin__: `net.trino.excel.plugin.ExcelPlugin`
4. __Configuration__
    * __Configuration Class__: Create a POJO class to hold your connector's configuration properties (e.g.,
      `ExcelConfig.java`). Use annotations like `@Config("excel.zip-url")` to map properties from the catalog file.

    * __Catalog Properties File__: Users will configure your connector via a properties file in `etc/catalog/` (
      e.g., `excel.properties`).

    ```java
    public class ExcelConfig {
        private URL excelZipUrl;

        @NotNull
        public URL getExcelZipUrl() {
            return excelZipUrl;
        }

        @Config("excel.zip.url")
        public ExcelConfig setExcelZipUrl(String excelZipUrlString) {
            try {
                this.excelZipUrl = new URI(excelZipUrlString).toURL();
            } catch (URISyntaxException | MalformedURLException e) {
                throw new IllegalArgumentException(
                        String.format("Invalid URL format for excel.zip.url: %s", excelZipUrlString), e);
            }
            return this;
        }
    }
    ```

5. __Dependency Injection with Guice__
   Trino uses Google Guice for dependency injection.
    * __Guice Module__: Create a class that implements `com.google.inject.Module` (e.g., `ExcelModule.java`).
        * In the `configure(Binder binder)` method, you bind interfaces to their implementations (
          e.g., `binder.bind(ConnectorMetadata.class).to(ExcelMetadata.class).in(Scopes.SINGLETON);`).
        * You also bind your configuration class
          using `ConfigBinder.configBinder(binder).bindConfig(ExcelConfig.class);`.
    * __`@Inject` Annotation__: Use `@jakarta.inject.Inject` (or `com.google.inject.Inject`) on constructors to declare
      dependencies. Guice will then create and provide instances of these dependencies.

6. __Data Representation__: Handles and Splits
    * __ConnectorTableHandle__: An object that uniquely identifies a table within your connector. It's created by
      `ConnectorMetadata.getTableHandle` and passed around. (e.g., `ExcelTableHandle` stores schema/filename and
      table/sheet name).
    * __ColumnHandle__: An object that uniquely identifies a column within your connector. (e.g., `ExcelColumnHandle`
      stores column name, type, and its ordinal position/index).
    * __ConnectorSplit__: Represents a portion of data from a table. Contains enough information for
      the `ConnectorRecordSetProvider` to read that portion. (e.g., `ExcelSplit` contains the table handle and
      potentially other details like the ZIP URL or specific file path if already extracted).

7. __Type System__
    * Trino has its own type system (`io.trino.spi.type.Type` and its implementations
      like `VarcharType`, `BigintType`, `DoubleType`, `TimestampWithTimeZoneType`, etc.).
    * __TODO__ RecordCursor must return data in the Trino type format (e.g., `long` for `BigintType`, `double`
      for `DoubleType`, `Slice` for `VarcharType`).
    * __TODO__ `ConnectorMetadata` declares the Trino types for columns. The Excel plugin currently simplifies this by
      treating all columns as `VarcharType.VARCHAR`.

8. __Error Handling__
    * Throw `io.trino.spi.TrinoException` for errors.
    * Define custom error codes by creating an `ErrorCodeSupplier` (e.g., `ExcelErrorCode.java`). This helps in
      providing clear, categorized error messages.

   ```properties
    connector.name=excel
    excel.zip-url=http://example.com/data.zip
   ```

9. __Building and Testing__
    * __Gradle__: Use Gradle for building. The shadowJar task is crucial for packaging.
    * __Unit Tests__: Use JUnit and Mockito to test individual components in isolation.
    * __Integration Tests__: Trino provides a testing framework (`io.trino:trino-testing`). Testcontainers is also
      excellent for setting up Trino instances and external dependencies (like an Nginx server for the Excel plugin)
      programmatically. (e.g., `ExcelConnectorIT.java`).

10. __Key Files in the Excel Plugin to Study__
    * __ExcelPlugin.java__: Main plugin entry point.
    * __ExcelConnectorFactory.java__: Creates the connector instance.
    * __ExcelModule.java__: Guice bindings.
    * __ExcelConfig.java__: Configuration properties.
    * __ExcelConnector.java__: Core connector logic.
    * __ExcelMetadata.java__: Metadata handling.
    * __ExcelSplitManager.java__: Data splitting.
    * `ExcelRecordSetProvider.java`, `ExcelRecordSet.java`, `ExcelRecordCursor.java`: Data reading.
    * __Handle classes__: ExcelTableHandle.java, ExcelColumnHandle.java.
    * __Split class__: ExcelSplit.java.
    * __Error codes__: ExcelErrorCode.java.
    * __Utilities__: ExcelFileDownloader.java, ExcelZipExtractor.java.
    * __Build file__: build.gradle.kts.
    * __Integration Test__: src/test/java/net/trino/excel/it/ExcelConnectorIT.java.

__NOTE__: _This guide should give you a solid foundation. Developing a Trino connector involves understanding these core
concepts
and implementing the SPI interfaces according to your data source's specific needs. The Excel connector, while
relatively simple in its data type handling, demonstrates many of these key patterns. Remember to consult the official
Trino documentation for more in-depth information and advanced features. However, this plugin development has only
focused on reading data (as a source) and does not include any implementation or support for sink functionality that is,
writing data back to a system._

__What is a Sink in Trino?__
_In Trino's architecture, a sink refers to the ability to write data to an external system, as opposed to a source which
only reads data.
Sink functionality is essential for operations such as `CREATE TABLE AS SELECT (CTAS)`, `INSERT INTO, and MERGE,`
where Trino is expected not just to read but also persist results into a destination table or data store.
To support sink operations, a Trino connector must implement specific SPI interfaces, including:_

* __ConnectorPageSinkProvider__
* __ConnectorPageSink__
* __ConnectorMetadata__ (with support for write operations)
* __ConnectorOutputTableHandle__, __ConnectorInsertTableHandle__, and sometimes __ConnectorMergeSinkHandle__

__Why is Sink Support Important?__
_Without sink support, a connector is read-only, which limits its use in data pipelines or analytical workflows that
require writing results back to a data source. For example, if you want to use Trino for ETL pipelines or to store
processed query results in a destination system, sink functionality is crucial._