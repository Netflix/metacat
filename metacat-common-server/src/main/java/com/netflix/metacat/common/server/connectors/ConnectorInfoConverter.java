package com.netflix.metacat.common.server.connectors;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.connectors.model.DatabaseInfo;
import com.netflix.metacat.common.server.connectors.model.PartitionInfo;
import com.netflix.metacat.common.server.connectors.model.TableInfo;

/**
 * Converter that converts Metacat dtos to connector represented types and vice versa.
 *
 * @param <D> Connector database type
 * @param <T> Connector table type
 * @param <P> Connector partition type
 * @author amajumdar
 */
public interface ConnectorInfoConverter<D, T, P> {
    /**
     * Standard error message for all default implementations.
     */
    String UNSUPPORTED_MESSAGE = "Not supported by this connector";

    /**
     * Converts to DatabaseDto.
     * @param qualifiedName qualifiedName
     * @param database connector database
     * @return Metacat database dto
     */
    default DatabaseInfo toDatabaseInfo(final QualifiedName qualifiedName, final D database) {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Converts from DatabaseDto to the connector database.
     *
     * @param database Metacat database dto
     * @return connector database
     */
    default D fromDatabaseInfo(final DatabaseInfo database) {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Converts to TableDto.
     * @param qualifiedName qualifiedName
     * @param table connector table
     * @return Metacat table dto
     */
    default TableInfo toTableInfo(final QualifiedName qualifiedName, final T table) {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Converts from TableDto to the connector table.
     *
     * @param table Metacat table dto
     * @return connector table
     */
    default T fromTableInfo(final TableInfo table) {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Converts to PartitionDto.
     * @param tableInfo tableInfo
     * @param partition connector partition
     * @return Metacat partition dto
     */
    default PartitionInfo toPartitionInfo(final TableInfo tableInfo, final P partition) {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Converts from PartitionDto to the connector partition.
     * @param tableInfo tableInfo
     * @param partition Metacat partition dto
     * @return connector partition
     */
    default P fromPartitionInfo(final TableInfo tableInfo, final PartitionInfo partition) {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }
}
