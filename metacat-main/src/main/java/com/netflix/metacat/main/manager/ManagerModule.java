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

package com.netflix.metacat.main.manager;

import com.facebook.presto.connector.informationSchema.InformationSchemaModule;
import com.facebook.presto.metadata.CatalogManagerConfig;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.RemoteSplitHandleResolver;
import com.facebook.presto.metadata.ViewDefinition;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorPageSourceProvider;
import com.facebook.presto.spi.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.split.PageSourceProvider;
import com.facebook.presto.type.TypeDeserializer;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.Maps;
import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.multibindings.MapBinder;
import com.netflix.metacat.main.connector.MetacatConnectorManager;
import com.netflix.metacat.main.presto.connector.ConnectorManager;
import com.netflix.metacat.main.presto.index.IndexManager;
import com.netflix.metacat.main.presto.metadata.HandleResolver;
import com.netflix.metacat.main.presto.metadata.MetadataManager;
import com.netflix.metacat.main.presto.split.PageSourceManager;
import com.netflix.metacat.main.presto.split.SplitManager;
import io.airlift.configuration.ConfigurationFactory;

import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigurationModule.bindConfig;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;

/**
 * Created by amajumdar on 1/14/15.
 */
public class ManagerModule extends AbstractModule {
    @Override
    protected void configure() {
        // Configuration factory
        binder().bind(ConfigurationFactory.class).toInstance(new ConfigurationFactory(Maps.newHashMap()));
        // Node manager
        binder().bind(NodeManager.class).to(InMemoryNodeManager.class).in(Scopes.SINGLETON);
        binder().bind(InternalNodeManager.class).to(InMemoryNodeManager.class).in(Scopes.SINGLETON);

        // split manager
        binder().bind(SplitManager.class).in(Scopes.SINGLETON);

        // data stream provider
        binder().bind(PageSourceManager.class).in(Scopes.SINGLETON);
        binder().bind(PageSourceProvider.class).to(PageSourceManager.class).in(Scopes.SINGLETON);
        newSetBinder(binder(), ConnectorPageSourceProvider.class);

        // record sink provider
        newSetBinder(binder(), ConnectorRecordSinkProvider.class);
        // metadata
        binder().bind(MetadataManager.class).in(Scopes.SINGLETON);
        binder().bind(Metadata.class).to(MetadataManager.class).in(Scopes.SINGLETON);

        // type
        binder().bind(TypeRegistry.class).in(Scopes.SINGLETON);
        binder().bind(TypeManager.class).to(TypeRegistry.class).in(Scopes.SINGLETON);
        jsonBinder(binder()).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
        newSetBinder(binder(), Type.class);

        // index manager
        binder().bind(IndexManager.class).in(Scopes.SINGLETON);

        // handle resolver
        binder().bind(HandleResolver.class).in(Scopes.SINGLETON);
        MapBinder<String, ConnectorHandleResolver> connectorHandleResolverBinder = newMapBinder(binder(), String.class,
                ConnectorHandleResolver.class);
        connectorHandleResolverBinder.addBinding("remote").to(RemoteSplitHandleResolver.class).in(Scopes.SINGLETON);

        // connector
        binder().bind(ConnectorManager.class).to(MetacatConnectorManager.class).in(Scopes.SINGLETON);
        newMapBinder(binder(), String.class, ConnectorFactory.class);

        // information schema
        binder().install(new InformationSchemaModule());

        // json codec
        jsonCodecBinder(binder()).bindJsonCodec(ViewDefinition.class);

        //
        bindConfig(binder()).to(CatalogManagerConfig.class);
    }
}
