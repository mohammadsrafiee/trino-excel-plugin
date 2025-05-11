package net.trino.excel.plugin;

import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import java.util.List;

/**
 * The {@code ExcelPlugin} class is a Trino plugin that provides support for connecting to Excel data sources. This
 * plugin registers a single {@link ConnectorFactory}, which is responsible for creating connectors that can read from
 * Excel files. The connector factory is implemented by {@link ExcelPluginFactory}. To use this plugin, it should be
 * included in Trino's plugin directory and configured via the appropriate catalog properties.
 *
 * @see io.trino.spi.Plugin
 * @see ExcelPluginFactory
 */
public class ExcelPlugin implements Plugin {

    /**
     * Returns a list containing a single {@link ConnectorFactory} instance, used to create connectors for Excel data
     * sources.
     *
     * @return an iterable containing the {@link ExcelPluginFactory}
     */
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories() {
        return List.of(new ExcelPluginFactory());
    }
}
