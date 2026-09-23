/*
 *
 *  Copyright 2021 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.netflix.metacat.connector.polaris.configs;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;

import javax.sql.DataSource;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * JVM-scoped registry that ensures all Polaris catalogs pointing at the same database URL
 * share a single HikariCP connection pool rather than creating one per catalog.
 *
 * Pools registered here are never destroyed by individual catalog context shutdowns.
 */
public final class PolarisDataSourceRegistry {

    private static final ConcurrentHashMap<String, HikariDataSource> PRIMARY = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, HikariDataSource> READER = new ConcurrentHashMap<>();

    private PolarisDataSourceRegistry() {
    }

    /**
     * Returns a shared primary DataSource for the given URL, creating it on first call.
     *
     * @param url    JDBC URL used as the cache key
     * @param config flat property map from the connector context
     * @return shared HikariDataSource
     */
    public static DataSource getOrCreatePrimary(final String url, final Map<String, String> config) {
        return PRIMARY.computeIfAbsent(url, k -> build(config, "spring.datasource"));
    }

    /**
     * Returns a shared reader DataSource for the given URL, creating it on first call.
     *
     * @param url    JDBC URL used as the cache key
     * @param config flat property map from the connector context
     * @return shared HikariDataSource
     */
    public static DataSource getOrCreateReader(final String url, final Map<String, String> config) {
        return READER.computeIfAbsent(url, k -> build(config, "spring.datasource.reader"));
    }

    private static HikariDataSource build(final Map<String, String> config, final String prefix) {
        final StandardEnvironment env = new StandardEnvironment();
        env.getPropertySources().addFirst(
            new MapPropertySource("polaris_registry", Collections.unmodifiableMap(config)));
        final Binder binder = Binder.get(env);

        final DataSourceProperties props = binder
            .bind(prefix, Bindable.of(DataSourceProperties.class))
            .orElse(new DataSourceProperties());

        final HikariDataSource ds = props.initializeDataSourceBuilder()
            .type(HikariDataSource.class)
            .build();

        binder.bind(prefix + ".hikari", Bindable.ofInstance(ds));
        return ds;
    }
}
