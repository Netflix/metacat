package com.netflix.metacat.shema.converters;

import com.google.inject.AbstractModule;

/**
 * Schema converter module.
 */
public class SchemaConverterModule extends AbstractModule {
    @Override
    protected void configure() {
        binder().bind(HiveMetacatConverters.class).to(HiveMetacatConvertersImpl.class);
    }
}
