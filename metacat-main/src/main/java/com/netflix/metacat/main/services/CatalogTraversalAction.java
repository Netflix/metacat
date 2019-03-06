/*
 *  Copyright 2019 Netflix, Inc.
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

package com.netflix.metacat.main.services;

import com.netflix.metacat.common.dto.CatalogDto;
import com.netflix.metacat.common.dto.DatabaseDto;
import com.netflix.metacat.common.dto.TableDto;

import java.util.List;
import java.util.Optional;

/**
 * Action interface that will be called on catalog traversal.
 */
public interface CatalogTraversalAction {
    /**
     * Called when the catalog traversal starts.
     *
     * @param context traversal context
     */
    default void init(CatalogTraversal.Context context) { }

    /**
     * Called when the catalog traversal processes catalogs.
     *
     * @param context traversal context
     * @param catalogs list of catalogs
     */
    default void applyCatalogs(CatalogTraversal.Context context, List<CatalogDto> catalogs) { }

    /**
     * Called when the catalog traversal processes databases.
     *
     * @param context traversal context
     * @param databases lst of databases
     */
    default void applyDatabases(CatalogTraversal.Context context, List<DatabaseDto> databases) { }

    /**
     * Called when the catalog traversal processes tables.
     *
     * @param context traversal context
     * @param tables list of tables
     */
    default void applyTables(CatalogTraversal.Context context, List<Optional<TableDto>> tables) { }

    /**
     * Called when the catalog traversal ends.
     *
     * @param context traversal context
     */
    default void done(CatalogTraversal.Context context) { }
}
