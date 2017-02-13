package com.netflix.metacat.main.manager;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.netflix.metacat.common.type.TypeManager;
import com.netflix.metacat.common.type.TypeRegistry;

/**
 * Guice module for Manager classes.
 *
 * @author amajumdar
 */
public class ManagerModule extends AbstractModule {
    @Override
    protected void configure() {
        binder().bind(CatalogManager.class).in(Scopes.SINGLETON);
        binder().bind(PluginManager.class).in(Scopes.SINGLETON);
        binder().bind(ConnectorManager.class).in(Scopes.SINGLETON);

        binder().bind(TypeManager.class).toInstance(TypeRegistry.getTypeRegistry());
    }
}
