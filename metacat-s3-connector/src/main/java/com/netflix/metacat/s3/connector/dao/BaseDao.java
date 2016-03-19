package com.netflix.metacat.s3.connector.dao;

import java.util.List;

/**
 * The base dao.
 *
 * @param <T>
 */
public interface BaseDao<T> {

    /**
     * Save the entity to the data store.
     * @param entity the entity to save.
     */
    public T save( T entity);

    /**
     * Save the entity and refresh the entity from
     * the database if required.
     *
     * @param entity the entity to be saved and refreshed.
     *
     * @param isRefreshRequired {@code true} to perform a refresh from the store.
     */
    public T save(T entity, boolean isRefreshRequired);

    /**
     * Saves all given entities.
     *
     * @param entities
     * @return the saved entities
     * @throws IllegalArgumentException in case the given entity is (@literal null}.
     */
    public List<T> save( Iterable<T> entities);

    /**
     * Delete the entity by using the id,
     * @param id the id of the entity.
     */
    public void deleteById(Long id);
    /**
     * Delete the entities for the given ids.
     * @param ids list of ids.
     */
    public void deleteById(Iterable<Long> ids);
    /**
     * Delete the given entity
     * @param entity
     */
    public void delete(T entity);
    /**
     * Delete the given entities
     * @param entities
     */
    public void delete( Iterable<T> entities);
    /**
     * Deletes all entities managed by the repository.
     */
    public void deleteAll();
    /**
     * Returns whether an entity with the given id exists.
     *
     * @param id must not be {@literal null}.
     * @return true if an entity with the given id exists, {@literal false} otherwise
     * @throws IllegalArgumentException if {@code id} is {@literal null}
     */
    public boolean isExists(Long id);

    /**
     * Returns an entity for the given id
     * @param id  id of the entity
     * @return
     */
    public T get(Long id);
    /**
     * Returns an entity for the given name
     * @param name name of the entity
     * @return
     */
    public T getByName(String name);
    /**
     * Returns a list of entities for the given names
     * @param names names of the entities
     * @return
     */
    public List<T> getByNames(List<String> names);
    /**
     * Returns an entity for the given id
     * @param ids  list of ids
     * @return
     */
    public List<T> get(Iterable<Long> ids);
    /**
     * Returns all the instances
     * @return  Returns all the instances
     */
    public List<T> getAll();

    /**
     * Returns the number of entities available.
     *
     * @return the number of entities
     */
    long count();

}

