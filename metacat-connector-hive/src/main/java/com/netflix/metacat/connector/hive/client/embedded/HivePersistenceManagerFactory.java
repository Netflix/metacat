/*
 *  Copyright 2017 Netflix, Inc.
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
 */

package com.netflix.metacat.connector.hive.client.embedded;

import com.google.common.collect.Maps;
import com.netflix.metacat.common.server.util.DataSourceManager;
import com.netflix.metacat.connector.hive.util.HiveConfigConstants;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;

import javax.sql.DataSource;
import java.util.Map;

/**
 * HivePersistenceManagerFactory.
 *
 * Manages Hibernate SessionFactory instances based on application configurations.
 *
 * @since 1.0.0
 */
@Slf4j
public final class HivePersistenceManagerFactory {
    private static Map<String, SessionFactory> factories = Maps.newConcurrentMap();

    private HivePersistenceManagerFactory() {
    }

    /**
     * getPersistenceManagerFactory.
     *
     * @param props props
     * @return SessionFactory
     */
    public static SessionFactory getPersistenceManagerFactory(final Map props) {
        final String name = String.valueOf(props.get(HiveConfigConstants.JAVAX_JDO_OPTION_NAME));
        SessionFactory result = factories.get(name);
        if (result == null) {
            result = createSessionFactory(name, props);
        }
        return result;
    }

    private static synchronized SessionFactory createSessionFactory(final String name, final Map props) {
        SessionFactory result = factories.get(name);
        if (result == null) {
            final DataSource dataSource = DataSourceManager.get().get(name);

            // Configure Hibernate SessionFactory
            Configuration configuration = new Configuration();
            configuration.setProperty("hibernate.connection.datasource", dataSource.toString());
            configuration.setProperty("hibernate.dialect", "org.hibernate.dialect.MySQL8Dialect");
            configuration.setProperty(HiveConfigConstants.HIBERNATE_HBM2DDL_AUTO, "update");
            configuration.setProperty(HiveConfigConstants.HIBERNATE_JDBC_TIMEOUT, "30");
            configuration.setProperty(HiveConfigConstants.HIBERNATE_CONNECTION_ISOLATION, "2"); // TRANSACTION_READ_COMMITTED
            configuration.setProperty(HiveConfigConstants.HIBERNATE_CACHE_USE_SECOND_LEVEL_CACHE, "false");
            configuration.setProperty(HiveConfigConstants.HIBERNATE_CACHE_REGION_FACTORY_CLASS, "org.hibernate.cache.internal.NoCachingRegionFactory");


            result = configuration.buildSessionFactory();
            factories.put(name, result);
        }
        return result;
    }
}
