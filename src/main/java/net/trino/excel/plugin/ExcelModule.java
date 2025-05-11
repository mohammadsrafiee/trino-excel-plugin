package net.trino.excel.plugin;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.configuration.ConfigBinder;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.type.TypeManager;
import net.trino.excel.communicator.ExcelConnector;
import net.trino.excel.communicator.ExcelMetadata;
import net.trino.excel.split.ExcelRecordSetProvider;
import net.trino.excel.split.ExcelSplitManager;

/**
 * The {@code ExcelModule} class is a Guice module used by the Trino Excel plugin to bind necessary services,
 * configurations, and components into the plugin's dependency injection context. This module registers and configures
 * core services such as:
 * <ul>
 *     <li>{@link ExcelConnector} - the connector implementation</li>
 *     <li>{@link ExcelSplitManager} - responsible for managing data splits</li>
 *     <li>{@link ExcelConfig} - configuration class for Excel connector settings</li>
 * </ul>
 *
 * <p>The class also injects essential Trino services like {@link NodeManager} and {@link TypeManager}
 * and ensures they are available to components that need them.</p>
 * <p>Note: Some components (related to OpenAPI integration) are currently commented out and
 * not part of the module's active bindings.</p>
 *
 * @see ExcelConnector
 * @see ExcelSplitManager
 * @see ExcelConfig
 */
public class ExcelModule implements Module {

    @Override
    public void configure(Binder binder) {
        ConfigBinder.configBinder(binder).bindConfig(ExcelConfig.class);
        binder.bind(ExcelMetadata.class).in(Scopes.SINGLETON);
        binder.bind(ExcelSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ExcelRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorMetadata.class).to(ExcelMetadata.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorSplitManager.class).to(ExcelSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorRecordSetProvider.class).to(ExcelRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(ExcelConnector.class).in(Scopes.SINGLETON);
    }
}
