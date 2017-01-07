package com.netflix.metacat.shema.converters;

import com.google.inject.AbstractModule;

/**
 * Created by zhenl on 1/7/17.
 */
public class SchemaConverterModule extends AbstractModule {
    @Override
    protected void configure() {
        binder().bind(HiveMetacatConverters.class).to(HiveMetacatConvertersImpl.class);
    }
}
