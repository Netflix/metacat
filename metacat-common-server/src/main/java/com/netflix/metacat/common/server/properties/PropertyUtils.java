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
package com.netflix.metacat.common.server.properties;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.netflix.metacat.common.QualifiedName;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utility methods for working with properties.
 *
 * @author tgianos
 * @since 1.1.0
 */
public final class PropertyUtils {

    /**
     * Protected constructor for utility class.
     */
    protected PropertyUtils() {
    }

    /**
     * Convert a delimited string into a List of {@code QualifiedName}
     *
     * @param names     The list of names to split
     * @param delimiter The delimiter to use for splitting
     * @return The list of qualified names
     */
    static List<QualifiedName> delimitedStringsToQualifiedNamesList(
        @Nonnull @NonNull final String names,
        final char delimiter
    ) {
        if (StringUtils.isNotBlank(names)) {
            return Splitter.on(delimiter)
                .omitEmptyStrings()
                .splitToList(names).stream()
                .map(QualifiedName::fromString)
                .collect(Collectors.toList());
        } else {
            return Lists.newArrayList();
        }
    }
}
