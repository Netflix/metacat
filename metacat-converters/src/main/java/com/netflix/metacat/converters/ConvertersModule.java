package com.netflix.metacat.converters;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.metacat.converters.impl.DateConverters;
import com.netflix.metacat.converters.impl.HiveTypeConverter;
import com.netflix.metacat.converters.impl.MapStructHiveConverters;
import com.netflix.metacat.converters.impl.MapStructPrestoConverters;
import com.netflix.metacat.converters.impl.PigTypeConverter;
import com.netflix.metacat.converters.impl.PrestoTypeConverter;
import org.mapstruct.factory.Mappers;

import javax.inject.Provider;

public class ConvertersModule extends AbstractModule {
    @Override
    protected void configure() {
        binder().bind(HiveTypeConverter.class).asEagerSingleton();
        binder().bind(TypeConverterProvider.class).asEagerSingleton();
        binder().bind(TypeConverter.class).toProvider(TypeConverterProvider.class);
        binder().bind(PigTypeConverter.class).asEagerSingleton();
        binder().bind(PrestoTypeConverter.class).asEagerSingleton();

        // This is a bad pattern, but I am doing it as a first step so we can abstract out configuration lookup
        binder().requestStaticInjection(DateConverters.class);
    }

    @Provides
    public HiveConverters hiveConverters() {
        return Mappers.getMapper(MapStructHiveConverters.class);
    }

    @Provides
    public PrestoConverters prestoConverters(Provider<TypeConverter> typeConverterProvider) {
        MapStructPrestoConverters converters = Mappers.getMapper(MapStructPrestoConverters.class);
        converters.setTypeConverter(typeConverterProvider);
        return converters;
    }
}
