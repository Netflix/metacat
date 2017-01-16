/*
 *  Copyright 2016 Netflix, Inc.
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *         http://www.apache.org/licenses/LICENSE-2.0
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */

package com.netflix.metacat.canonical.common.spi;


import com.netflix.metacat.canonical.common.spi.util.LocalProperty;
import com.netflix.metacat.canonical.common.spi.util.TupleDomain;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * connector table layout.
 * @author zhenl
 */
@RequiredArgsConstructor
@Getter
@EqualsAndHashCode
public class ConnectorTableLayout {
    @NonNull
    private final ConnectorTableLayoutHandle handle;
    @NonNull
    private final Optional<List<ColumnHandle>> columns;
    @NonNull
    private final TupleDomain<ColumnHandle> predicate;
    @NonNull
    private final Optional<List<TupleDomain<ColumnHandle>>> discretePredicates;
    @NonNull
    private final Optional<Set<ColumnHandle>> partitioningColumns;
    @NonNull
    private final List<LocalProperty<ColumnHandle>> localProperties;
}
