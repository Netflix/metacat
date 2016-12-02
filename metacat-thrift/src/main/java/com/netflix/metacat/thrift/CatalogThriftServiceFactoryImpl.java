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

/**
 * Thrift service factory.
 */
public class CatalogThriftServiceFactoryImpl implements CatalogThriftServiceFactory {
    private final Config config;
    private final HiveConverters hiveConverters;
    private final MetacatV1 metacatV1;
    private final PartitionV1 partitionV1;
    private final TypeConverterProvider typeConverterProvider;
    private final LoadingCache<CacheKey, CatalogThriftService> cache = CacheBuilder.newBuilder()
        .build(new CacheLoader<CacheKey, CatalogThriftService>() {
            public CatalogThriftService load(final CacheKey key) {
                return new CatalogThriftService(config, typeConverterProvider, hiveConverters, metacatV1,
                    partitionV1, key.catalogName, key.portNumber);
            }
        });

    /**
     * Constructor.
     * @param config config
     * @param typeConverterProvider type converter provider
     * @param hiveConverters hive converter
     * @param metacatV1 Metacat V1
     * @param partitionV1 Partition V1
     */
    @Inject
    public CatalogThriftServiceFactoryImpl(final Config config, final TypeConverterProvider typeConverterProvider,
        final HiveConverters hiveConverters, final MetacatV1 metacatV1, final PartitionV1 partitionV1) {
        this.config = config;
        this.typeConverterProvider = typeConverterProvider;
        this.hiveConverters = hiveConverters;
        this.metacatV1 = metacatV1;
        this.partitionV1 = partitionV1;
    }

    @Override
    public CatalogThriftService create(final String catalogName, final int portNumber) {
        return cache.getUnchecked(new CacheKey(catalogName, portNumber));
    }

    private static final class CacheKey {
        private final String catalogName;
        private final int portNumber;

        private CacheKey(final String catalogName, final int portNumber) {
            this.catalogName = catalogName;
            this.portNumber = portNumber;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof CacheKey)) {
                return false;
            }
            final CacheKey cacheKey = (CacheKey) o;
            return Objects.equals(portNumber, cacheKey.portNumber) && Objects.equals(catalogName, cacheKey.catalogName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(catalogName, portNumber);
        }
    }
}
