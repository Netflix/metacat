package com.netflix.metacat.common.server;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
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
import com.netflix.metacat.common.server.events.DeadEventHandler;
import com.netflix.metacat.common.server.events.MetacatEventBus;
import com.netflix.metacat.common.util.DataSourceManager;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class CommonModule extends AbstractModule {
    @Override
    protected void configure() {
        Config config = new ArchaiusConfigImpl();

        bind(Config.class).toInstance(config);
        bind(MetacatJson.class).toInstance(MetacatJsonLocator.INSTANCE);
        bind(DeadEventHandler.class).asEagerSingleton();
        bind(DataSourceManager.class).toInstance(DataSourceManager.get());
        MetacatEventBus eventBus = createMetacatEventBus(config);
        bind(MetacatEventBus.class).toInstance(eventBus);
        bindListener(Matchers.any(), new TypeListener() {
            public <I> void hear(TypeLiteral<I> typeLiteral, TypeEncounter<I> typeEncounter) {
                typeEncounter.register((InjectionListener<I>) eventBus::register);
            }
        });

        // Injecting statics is a bad pattern and should be avoided, but I am doing it as a first step to allow
        // us to remove the hard coded username.
        binder().requestStaticInjection(Lookup.class, TagItem.class);
    }

    protected MetacatEventBus createMetacatEventBus(Config config) {
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("metacat-event-pool-%d").build();
        int threadCount = config.getEventBusThreadCount();
        return new MetacatEventBus("metacat-event-bus", Executors.newFixedThreadPool(threadCount, threadFactory));
    }
}
