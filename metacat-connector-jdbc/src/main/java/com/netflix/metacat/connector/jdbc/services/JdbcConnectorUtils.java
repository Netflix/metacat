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
package com.netflix.metacat.connector.jdbc.services;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.Sort;
import lombok.NonNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utility methods for working with JDBC connections.
 *
 * @author tgianos
 * @since 0.1.52
 */
public final class JdbcConnectorUtils {

    /**
     * Utility class constructor private.
     */
    private JdbcConnectorUtils() {
    }

    /**
     * Execute a SQL update statement against the given datasource.
     *
     * @param connection The connection to attempt to execute an update against
     * @param sql        The sql to execute
     * @return The number of rows updated or exception
     */
    static int executeUpdate(@Nonnull @NonNull final Connection connection, @Nonnull @NonNull final String sql) {
        try (final Statement statement = connection.createStatement()) {
            return statement.executeUpdate(sql);
        } catch (final SQLException se) {
            throw Throwables.propagate(se);
        }
    }

    /**
     * Sort the Qualified Names using the comparator in the desired order.
     *
     * @param names      The qualified names to sort
     * @param sort       The sort object defining ascending or descending order
     * @param comparator The comparator to use
     */
    public static void sort(
        @Nonnull @NonNull final List<QualifiedName> names,
        @Nonnull @NonNull final Sort sort,
        @Nonnull @NonNull final Comparator<QualifiedName> comparator
    ) {
        switch (sort.getOrder()) {
            case DESC:
                names.sort(comparator.reversed());
                break;
            case ASC:
            default:
                names.sort(comparator);
        }
    }

    /**
     * If the user desires pagination this method will take the list and break it up into the correct chunk. If not it
     * will return the whole list.
     *
     * @param names    The names to paginate
     * @param pageable The pagination parameters or null if no pagination required
     * @return The final list of qualified names
     */
    public static List<QualifiedName> paginate(
        @Nonnull @NonNull final List<QualifiedName> names,
        @Nullable final Pageable pageable
    ) {
        final ImmutableList.Builder<QualifiedName> results = ImmutableList.builder();
        if (pageable != null && pageable.isPageable()) {
            results.addAll(
                names
                    .stream()
                    .skip(pageable.getOffset() * pageable.getLimit())
                    .limit(pageable.getLimit())
                    .collect(Collectors.toList())
            );
        } else {
            results.addAll(names);
        }

        return results.build();
    }
}
