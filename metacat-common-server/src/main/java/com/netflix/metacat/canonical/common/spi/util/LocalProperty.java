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

package com.netflix.metacat.canonical.common.spi.util;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * class of local property.
 *
 * @param <E>
 * @author zhenl
 */
public interface LocalProperty<E> {
    /**
     * translate.
     *
     * @param translator translate
     * @param <T>        t
     * @return optional.
     */
    <T> Optional<LocalProperty<T>> translate(Function<E, Optional<T>> translator);

    /**
     * Return true if the actual LocalProperty can be used to simplify this LocalProperty.
     *
     * @param actual actual
     * @return boolean
     */
    boolean isSimplifiedBy(LocalProperty<E> actual);

    /**
     * Simplfies this LocalProperty provided that the specified inputs are constants.
     *
     * @param constants constants
     * @return optional
     */
    default Optional<LocalProperty<E>> withConstants(final Set<E> constants) {
        final Set<E> set = new HashSet<>(getColumns());
        set.removeAll(constants);

        if (set.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(constrain(set));
    }

    /**
     * Return a new instance with the give (reduced) set of columns.
     *
     * @param columns columns
     * @return property
     */
    default LocalProperty<E> constrain(final Set<E> columns) {
        if (!columns.equals(getColumns())) {
            throw new IllegalArgumentException(String.format("Cannot constrain %s with %s", this, columns));
        }
        return this;
    }

    /**
     * get columns.
     *
     * @return columns.
     */
    Set<E> getColumns();
}
