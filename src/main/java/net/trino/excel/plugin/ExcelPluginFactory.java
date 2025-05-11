package net.trino.excel.plugin;

import static java.util.Objects.requireNonNull;

import io.airlift.bootstrap.Bootstrap;
import io.airlift.log.Logger;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import java.util.Map;
import net.trino.excel.communicator.ExcelConnector;

/**
 * A factory for creating instances of the Excel Trino connector. This class is registered as the plugin entry point and
 * is responsible for initializing the connector with required configuration and dependency injection via
 * {@link ExcelModule}. Expected configuration includes properties such as:
 * <ul>
 *   <li>Path or URL to the Excel ZIP archive</li>
 *   <li>Any custom connector properties specific to Excel reading</li>
 * </ul>
 * Used by Trino during connector bootstrap.
 */
public class ExcelPluginFactory implements ConnectorFactory {

    private static final Logger log = Logger.get(ExcelPluginFactory.class);

    /**
     * The name used to identify this connector when configuring Trino catalogs.
     */
    public static final String CONNECTOR_NAME = "excel";

    /**
     * Returns the unique name of the connector.
     *
     * @return connector name ("excel")
     */
    @Override
    public String getName() {
        return CONNECTOR_NAME;
    }

    /**
     * Creates and initializes a new Excel connector instance.
     *
     * @param catalogName the name of the catalog
     * @param config      the configuration map containing required connector properties
     * @param context     the connector context provided by Trino
     * @return a fully configured {@link Connector} instance
     * @throws NullPointerException if any required parameter is null
     */
    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
        requireNonNull(config, "required-config is null.");
        requireNonNull(catalogName, "catalog-name is null");

        log.info("Creating Excel connector for catalog '%s'", catalogName);
        return new Bootstrap(new ExcelModule())
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config)
                .initialize()
                .getInstance(ExcelConnector.class);
    }
}
