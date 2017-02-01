package com.netflix.metacat.common.server.connectors;

import com.netflix.metacat.common.dto.DatabaseDto;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.TableDto;

/**
 * Converter that converts Metacat dtos to connector represented types and vice versa.
 * @param <D> Connector database type
 * @param <T> Connector table type
 * @param <P> Connector partition type
 * @author amajumdar
 */
public interface ConnectorDtoConverter<D, T, P> {
    /**
     * Standard error message for all default implementations.
     */
    String UNSUPPORTED_MESSAGE = "Not supported by this connector";

    /**
     * Converts to DatabaseDto.
     * @param database connector database
     * @return Metacat database dto
     */
    default DatabaseDto toDatabaseDto(final D database) {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Converts from DatabaseDto to the connector database.
     * @param database Metacat database dto
     * @return connector database
     */
    default D fromDatabaseDto(final DatabaseDto database) {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Converts to TableDto.
     * @param table connector table
     * @return Metacat table dto
     */
    default TableDto toTableDto(final T table) {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Converts from TableDto to the connector table.
     * @param table Metacat table dto
     * @return connector table
     */
    default T fromTableDto(final TableDto table) {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Converts to PartitionDto.
     * @param partition connector partition
     * @return Metacat partition dto
     */
    default PartitionDto toPartitionDto(final P partition) {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Converts from PartitionDto to the connector partition.
     * @param partition Metacat partition dto
     * @return connector partition
     */
    default P fromPartitionDto(final PartitionDto partition) {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }
}
