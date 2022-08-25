package com.netflix.metacat.main.manager;

import org.springframework.context.ApplicationContext;

/**
 * Interface that defines how catalogs should be loaded.
 */
public interface CatalogManager {

    /**
     * Flag indicating whether all catalogs have been loaded.
     *
     * @return True if they've been loaded.
     */
    boolean areCatalogsLoaded();

    /**
     * Load the catalogs for this applicationContext.
     *
     * @param applicationContext The application context.
     * @throws Exception exception if we failed to load a catalog.
     */
    void loadCatalogs(ApplicationContext applicationContext) throws Exception;
}

