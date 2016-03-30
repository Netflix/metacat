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

package com.netflix.metacat.main.services;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.netflix.metacat.main.services.impl.CatalogServiceImpl;
import com.netflix.metacat.main.services.impl.DatabaseServiceImpl;
import com.netflix.metacat.main.services.impl.MViewServiceImpl;
import com.netflix.metacat.main.services.impl.PartitionServiceImpl;
import com.netflix.metacat.main.services.impl.TableServiceImpl;
import com.netflix.metacat.main.services.search.ElasticSearchClientProvider;
import com.netflix.metacat.main.services.search.ElasticSearchMetacatRefresh;
import com.netflix.metacat.main.services.search.ElasticSearchUtil;
import com.netflix.metacat.main.services.search.MetacatEventHandlers;
import org.elasticsearch.client.Client;

import javax.inject.Singleton;

public class ServicesModule extends AbstractModule {
    @Override
    protected void configure() {
        binder().bind(CatalogService.class).to(CatalogServiceImpl.class).in(Scopes.SINGLETON);
        binder().bind(DatabaseService.class).to(DatabaseServiceImpl.class).in(Scopes.SINGLETON);
        binder().bind(TableService.class).to(TableServiceImpl.class).in(Scopes.SINGLETON);
        binder().bind(PartitionService.class).to(PartitionServiceImpl.class).in(Scopes.SINGLETON);
        binder().bind(MViewService.class).to(MViewServiceImpl.class).in(Scopes.SINGLETON);
        binder().bind(MetacatServiceHelper.class).in(Scopes.SINGLETON);
        //search
        bind(Client.class).toProvider(ElasticSearchClientProvider.class).in(Singleton.class);
        binder().bind(MetacatEventHandlers.class).in(Singleton.class);
        binder().bind(ElasticSearchUtil.class).in(Singleton.class);
        binder().bind(ElasticSearchMetacatRefresh.class).in(Singleton.class);
    }
}
