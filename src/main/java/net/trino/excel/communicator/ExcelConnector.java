package net.trino.excel.communicator;

import static java.util.Objects.requireNonNull;

import com.google.inject.Inject;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;
import net.trino.excel.split.ExcelRecordSetProvider;
import net.trino.excel.split.ExcelSplitManager;

/**
 * ExcelConnector is a Trino Connector implementation that provides access to Excel-based data sources. It integrates
 * metadata handling, split management, and record set provisioning for reading Excel files through Trino's query
 * engine.
 */
public class ExcelConnector implements Connector {

    private final ExcelRecordSetProvider recordSetProvider;
    private final ExcelSplitManager splitManager;
    private final ExcelMetadata metadata;

    /**
     * Constructs an ExcelConnector with the provided metadata, split manager, and record set provider.
     *
     * @param metadata          the metadata handler for Excel files
     * @param splitManager      the manager responsible for creating data splits
     * @param recordSetProvider the provider for creating record sets from Excel splits
     */
    @Inject
    public ExcelConnector(
            ExcelMetadata metadata, ExcelSplitManager splitManager, ExcelRecordSetProvider recordSetProvider) {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "split-manager is null");
        this.recordSetProvider = requireNonNull(recordSetProvider, "record-set-provider is null");
    }

    /**
     * Begins a new transaction. ExcelConnector uses a stateless transaction handle.
     *
     * @param isolationLevel the isolation level (not used)
     * @param readOnly       whether the transaction is read-only
     * @param autoCommit     whether the transaction should auto-commit
     * @return a singleton instance of ExcelTransactionHandle
     */
    @Override
    public ConnectorTransactionHandle beginTransaction(
            IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit) {
        return ExcelTransactionHandle.INSTANCE;
    }

    /**
     * Returns the metadata implementation for this connector.
     *
     * @param session     the connector session
     * @param transaction the transaction handle
     * @return the connector metadata instance
     */
    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transaction) {
        return metadata;
    }

    /**
     * Returns the split manager which creates data splits for Excel files.
     *
     * @return the connector split manager
     */
    @Override
    public ConnectorSplitManager getSplitManager() {
        return splitManager;
    }

    /**
     * Returns the record set provider which reads records from Excel splits.
     *
     * @return the connector record set provider
     */
    @Override
    public ConnectorRecordSetProvider getRecordSetProvider() {
        return recordSetProvider;
    }
}
