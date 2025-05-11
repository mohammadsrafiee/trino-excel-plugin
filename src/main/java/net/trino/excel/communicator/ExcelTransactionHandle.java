package net.trino.excel.communicator;

import io.trino.spi.connector.ConnectorTransactionHandle;

/**
 * ExcelTransactionHandle is a singleton enum used to represent a stateless transaction in the Excel connector. In
 * Trino, each connector must implement a transaction handle that is used to identify and manage the state of a
 * transaction. Since the Excel connector is read-only and does not support transactional state, a single shared
 * instance {@link #INSTANCE} is used. This enum-based implementation is a common pattern for stateless connectors.
 *
 * <h3>Trino ConnectorTransactionHandle Overview:</h3>
 * In Trino, {@link ConnectorTransactionHandle} is a marker interface used by connectors to track transactions. Typical
 * implementations include:
 * <ul>
 *   <li><b>Stateless Enum Singleton</b> — used for read-only or stateless connectors (e.g., Excel, JMX)</li>
 *   <li><b>UUID-based classes</b> — used in connectors that support transactional state (e.g., Hive, Iceberg)</li>
 *   <li><b>Custom State Objects</b> — for connectors that manage complex transaction coordination (e.g., JDBC-based connectors)</li>
 * </ul>
 * This design allows Trino to handle transaction isolation and resource scoping per connector, even if the connector itself is stateless.
 */
public enum ExcelTransactionHandle implements ConnectorTransactionHandle {
    /**
     * Singleton instance of ExcelTransactionHandle, used by the Excel connector to represent a stateless transaction
     * context.
     */
    INSTANCE
}
