package com.netflix.metacat.connector.polaris.mappers;

/**
 * Entity to Info Mapper.
 *
 * @param <E> The entity type to map from.
 * @param <I> The info type to map to.
 */
public interface EntityToInfoMapper<E, I> {

    /**
     * Maps an Entity to the Info object.
     *
     * @param entity The entity to map from.
     * @return The result info object.
     */
    I toInfo(E entity);
}
