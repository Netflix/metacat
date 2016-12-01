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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import com.netflix.metacat.common.api.MetacatV1;
import com.netflix.metacat.common.api.PartitionV1;
import com.netflix.metacat.common.server.Config;
import com.netflix.metacat.converters.HiveConverters;
import com.netflix.metacat.converters.TypeConverterProvider;

import java.util.Objects;

public class CatalogThriftServiceFactoryImpl implements CatalogThriftServiceFactory {
    private final Config config;
    private final HiveConverters hiveConverters;
    private final MetacatV1 metacatV1;
    private final PartitionV1 partitionV1;
    private final TypeConverterProvider typeConverterProvider;
    private final LoadingCache<CacheKey, CatalogThriftService> cache = CacheBuilder.newBuilder()
        .build(new CacheLoader<CacheKey, CatalogThriftService>() {
            public CatalogThriftService load(CacheKey key) {
                return new CatalogThriftService(config, typeConverterProvider, hiveConverters, metacatV1,
                    partitionV1, key.catalogName, key.portNumber);
            }
        });

    @Inject
    public CatalogThriftServiceFactoryImpl(Config config, TypeConverterProvider typeConverterProvider,
        HiveConverters hiveConverters, MetacatV1 metacatV1, PartitionV1 partitionV1) {
        this.config = config;
        this.typeConverterProvider = typeConverterProvider;
        this.hiveConverters = hiveConverters;
        this.metacatV1 = metacatV1;
        this.partitionV1 = partitionV1;
    }

    @Override
    public CatalogThriftService create(String catalogName, int portNumber) {
        return cache.getUnchecked(new CacheKey(catalogName, portNumber));
    }

    private static class CacheKey {
        public final String catalogName;
        public final int portNumber;

        private CacheKey(String catalogName, int portNumber) {
            this.catalogName = catalogName;
            this.portNumber = portNumber;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (!(o instanceof CacheKey))
                return false;
            CacheKey cacheKey = (CacheKey) o;
            return Objects.equals(portNumber, cacheKey.portNumber) && Objects.equals(catalogName, cacheKey.catalogName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(catalogName, portNumber);
        }
    }
}
