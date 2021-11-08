package com.netflix.metacat.connector.polaris.mappers;

/**
 * Info to Entity mapper.
 *
 * @param <I> The info type to map from.
 * @param <E> The entity type to map to.
 */
public interface InfoToEntityMapper<I, E> {

    /**
     * Maps an info object to an entity object.
     *
     * @param info   The info object to map from.
     * @return The result entity object.
     */
    E toEntity(I info);
}
