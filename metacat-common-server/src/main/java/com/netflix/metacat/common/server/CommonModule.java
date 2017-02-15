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

package com.netflix.metacat.common.server;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import com.google.inject.matcher.Matchers;
import com.google.inject.spi.InjectionListener;
import com.google.inject.spi.TypeEncounter;
import com.google.inject.spi.TypeListener;
import com.netflix.metacat.common.json.MetacatJson;
import com.netflix.metacat.common.json.MetacatJsonLocator;
import com.netflix.metacat.common.model.Lookup;
import com.netflix.metacat.common.model.TagItem;
import com.netflix.metacat.common.server.connectors.ConnectorTypeConverter;
import com.netflix.metacat.common.server.converter.ConverterUtil;
import com.netflix.metacat.common.server.converter.DozerTypeConverter;
import com.netflix.metacat.common.server.converter.TypeConverterProvider;
import com.netflix.metacat.common.server.events.DeadEventHandler;
import com.netflix.metacat.common.server.events.MetacatEventBus;
import com.netflix.metacat.common.util.DataSourceManager;
import com.netflix.metacat.common.util.ThreadServiceManager;

/**
 * Guice module.
 */
public class CommonModule extends AbstractModule {
    @Override
    protected void configure() {
        final Config config = new ArchaiusConfigImpl();

        bind(Config.class).toInstance(config);
        bind(MetacatJson.class).toInstance(MetacatJsonLocator.INSTANCE);
        bind(DeadEventHandler.class).asEagerSingleton();
        bind(DataSourceManager.class).toInstance(DataSourceManager.get());
        final MetacatEventBus eventBus = new MetacatEventBus(config);
        bind(MetacatEventBus.class).toInstance(eventBus);
        bindListener(Matchers.any(), new TypeListener() {
            public <I> void hear(final TypeLiteral<I> typeLiteral, final TypeEncounter<I> typeEncounter) {
                typeEncounter.register((InjectionListener<I>) eventBus::register);
            }
        });
        bind(ConverterUtil.class).asEagerSingleton();
        bind(DozerTypeConverter.class).asEagerSingleton();
        binder().bind(ConnectorTypeConverter.class).toProvider(TypeConverterProvider.class);
        bind(TypeConverterProvider.class).asEagerSingleton();

        // Injecting statics is a bad pattern and should be avoided, but I am doing it as a first step to allow
        // us to remove the hard coded username.
        binder().requestStaticInjection(Lookup.class, TagItem.class);
        bind(ThreadServiceManager.class).asEagerSingleton();
    }
}
