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
import com.google.common.collect.ImmutableList;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.Sort;
import lombok.NonNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utility methods shared by all Connectors.
 *
 * @author tgianos
 * @since 1.0.0
 */
public class ConnectorUtils {

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

    /**
     * Protected constructor for utility class.
     */
    protected ConnectorUtils() {
    }

    /**
     * Sort the Qualified Names using the comparator in the desired order.
     *
     * @param <T>        The type of elements to sort
     * @param elements   The list to sort
     * @param sort       The sort object defining ascending or descending order
     * @param comparator The comparator to use
     */
    public static <T> void sort(
        @Nonnull @NonNull final List<T> elements,
        @Nonnull @NonNull final Sort sort,
        @Nonnull @NonNull final Comparator<T> comparator
    ) {
        switch (sort.getOrder()) {
            case DESC:
                elements.sort(comparator.reversed());
                break;
            case ASC:
            default:
                elements.sort(comparator);
        }
    }

    /**
     * If the user desires pagination this method will take the list and break it up into the correct chunk. If not it
     * will return the whole list.
     *
     * @param <T>      The type of elements to paginate
     * @param elements The elements to paginate
     * @param pageable The pagination parameters or null if no pagination required
     * @return The final list of qualified names
     */
    public static <T> List<T> paginate(
        @Nonnull @NonNull final List<T> elements,
        @Nullable final Pageable pageable
    ) {
        final ImmutableList.Builder<T> results = ImmutableList.builder();
        if (pageable != null && pageable.isPageable()) {
            results.addAll(
                elements
                    .stream()
                    .skip(pageable.getOffset())
                    .limit(pageable.getLimit())
                    .collect(Collectors.toList())
            );
        } else {
            results.addAll(elements);
        }

        return results.build();
    }

    /**
     * Get the database service class to use.
     *
     * @param configuration       The connector configuration
     * @param defaultServiceClass The default class to use if an override is not found
     * @return The database service class to use.
     */
    public static Class<? extends ConnectorDatabaseService> getDatabaseServiceClass(
        @Nonnull @NonNull final Map<String, String> configuration,
        @Nonnull @NonNull final Class<? extends ConnectorDatabaseService> defaultServiceClass
    ) {
        if (configuration.containsKey(DATABASE_SERVICE_CLASS_KEY)) {
            final String className = configuration.get(DATABASE_SERVICE_CLASS_KEY);
            return getServiceClass(className, ConnectorDatabaseService.class);
        } else {
            return defaultServiceClass;
        }
    }

    /**
     * Get the table service class to use.
     *
     * @param configuration       The connector configuration
     * @param defaultServiceClass The default class to use if an override is not found
     * @return The table service class to use.
     */
    public static Class<? extends ConnectorTableService> getTableServiceClass(
        @Nonnull @NonNull final Map<String, String> configuration,
        @Nonnull @NonNull final Class<? extends ConnectorTableService> defaultServiceClass
    ) {
        if (configuration.containsKey(TABLE_SERVICE_CLASS_KEY)) {
            final String className = configuration.get(TABLE_SERVICE_CLASS_KEY);
            return getServiceClass(className, ConnectorTableService.class);
        } else {
            return defaultServiceClass;
        }
    }

    /**
     * Get the partition service class to use.
     *
     * @param configuration       The connector configuration
     * @param defaultServiceClass The default class to use if an override is not found
     * @return The partition service class to use.
     */
    public static Class<? extends ConnectorPartitionService> getPartitionServiceClass(
        @Nonnull @NonNull final Map<String, String> configuration,
        @Nonnull @NonNull final Class<? extends ConnectorPartitionService> defaultServiceClass
    ) {
        if (configuration.containsKey(PARTITION_SERVICE_CLASS_KEY)) {
            final String className = configuration.get(PARTITION_SERVICE_CLASS_KEY);
            return getServiceClass(className, ConnectorPartitionService.class);
        } else {
            return defaultServiceClass;
        }
    }

    private static <S extends ConnectorBaseService> Class<? extends S> getServiceClass(
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
