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

package com.netflix.metacat.thrift;

import com.google.inject.AbstractModule;

/**
 * Guice module.
 */
public class ThriftModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(CatalogThriftServiceFactory.class).to(CatalogThriftServiceFactoryImpl.class).asEagerSingleton();
        bind(HiveConverters.class).to(HiveConvertersImpl.class).asEagerSingleton();
        // This is a bad pattern, but I am doing it as a first step so we can abstract out configuration lookup
        binder().requestStaticInjection(DateConverters.class);
    }
}
