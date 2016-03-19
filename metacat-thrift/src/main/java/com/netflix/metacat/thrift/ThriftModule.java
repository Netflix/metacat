package com.netflix.metacat.thrift;

import com.google.inject.AbstractModule;

public class ThriftModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(CatalogThriftServiceFactory.class).to(CatalogThriftServiceFactoryImpl.class).asEagerSingleton();
    }
}
