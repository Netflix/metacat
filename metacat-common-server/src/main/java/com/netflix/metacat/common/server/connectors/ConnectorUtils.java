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

import com.google.common.collect.ImmutableList;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.Sort;
import lombok.NonNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utility methods shared by all Connectors.
 *
 * @author tgianos
 * @since 1.0.0
 */
public class ConnectorUtils {

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
}
