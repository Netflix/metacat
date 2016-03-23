/*
 * Copyright 2016 Netflix, Inc.
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

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
