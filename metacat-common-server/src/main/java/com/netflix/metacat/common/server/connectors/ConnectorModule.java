/*
 *
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
 *
 */
package com.netflix.metacat.common.server.connectors;

import com.google.common.base.Throwables;
import com.google.inject.AbstractModule;
import lombok.NonNull;

import javax.annotation.Nonnull;
import java.util.Map;

/**
 * A module that provides some reusable helper methods for connectors.
 *
 * @author tgianos
 * @since 1.0.0
 */
public abstract class ConnectorModule extends AbstractModule {

    /**
     * The key which a user can set a value in a catalog to override the default database service class.
     */
    private static final String DATABASE_SERVICE_CLASS_KEY = "metacat.connector.databaseService.class";

    /**
     * The key which a user can set a value in a catalog to override the default table service class.
     */
    private static final String TABLE_SERVICE_CLASS_KEY = "metacat.connector.tableService.class";

    /**
     * The key which a user can set a value in a catalog to override the default partition service class.
     */
    private static final String PARTITION_SERVICE_CLASS_KEY = "metacat.connector.partitionService.class";

    protected Class<? extends ConnectorDatabaseService> getDatabaseServiceClass(
        @Nonnull @NonNull final Map<String, String> configuration,
        @Nonnull @NonNull final Class<? extends ConnectorDatabaseService> defaultServiceClass
    ) {
        if (configuration.containsKey(DATABASE_SERVICE_CLASS_KEY)) {
            final String className = configuration.get(DATABASE_SERVICE_CLASS_KEY);
            return this.getServiceClass(className, ConnectorDatabaseService.class);
        } else {
            return defaultServiceClass;
        }
    }

    protected Class<? extends ConnectorTableService> getTableServiceClass(
        @Nonnull @NonNull final Map<String, String> configuration,
        @Nonnull @NonNull final Class<? extends ConnectorTableService> defaultServiceClass
    ) {
        if (configuration.containsKey(TABLE_SERVICE_CLASS_KEY)) {
            final String className = configuration.get(TABLE_SERVICE_CLASS_KEY);
            return this.getServiceClass(className, ConnectorTableService.class);
        } else {
            return defaultServiceClass;
        }
    }

    protected Class<? extends ConnectorPartitionService> getPartitionServiceClass(
        @Nonnull @NonNull final Map<String, String> configuration,
        @Nonnull @NonNull final Class<? extends ConnectorPartitionService> defaultServiceClass
    ) {
        if (configuration.containsKey(PARTITION_SERVICE_CLASS_KEY)) {
            final String className = configuration.get(PARTITION_SERVICE_CLASS_KEY);
            return this.getServiceClass(className, ConnectorPartitionService.class);
        } else {
            return defaultServiceClass;
        }
    }

    private <S extends ConnectorBaseService> Class<? extends S> getServiceClass(
        @Nonnull @NonNull final String className,
        @Nonnull @NonNull final Class<? extends S> baseClass
    ) {
        try {
            return Class.forName(className).asSubclass(baseClass);
        } catch (final ClassNotFoundException cnfe) {
            throw Throwables.propagate(cnfe);
        }
    }
}
