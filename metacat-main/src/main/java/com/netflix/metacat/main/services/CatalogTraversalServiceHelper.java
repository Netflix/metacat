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

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.CatalogDto;
import com.netflix.metacat.common.dto.CatalogMappingDto;
import com.netflix.metacat.common.dto.DatabaseDto;
import com.netflix.metacat.common.dto.TableDto;
import lombok.NonNull;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Service helper class for catalog traversal.
 */
public class CatalogTraversalServiceHelper {
    protected final CatalogService catalogService;
    protected final TableService tableService;
    protected final DatabaseService databaseService;

    /**
     * Constructor.
     *
     * @param catalogService      Catalog service
     * @param databaseService     Database service
     * @param tableService        Table service
     */
    public CatalogTraversalServiceHelper(
        @Nonnull @NonNull final CatalogService catalogService,
        @Nonnull @NonNull final DatabaseService databaseService,
        @Nonnull @NonNull final TableService tableService
    ) {
        this.catalogService = catalogService;
        this.databaseService = databaseService;
        this.tableService = tableService;
    }

    /**
     * Returns the list of catalog names.
     * @return list of catalog names
     */
    public List<String> getCatalogNames() {
        return catalogService.getCatalogNames().stream().map(CatalogMappingDto::getCatalogName).collect(
            Collectors.toList());
    }

    /**
     * Returns the catalog for the given <code>name</code>.
     * @param catalogName catalog name
     * @return catalog
     */
    public CatalogDto getCatalog(final String catalogName) {
        return catalogService.get(QualifiedName.ofCatalog(catalogName));
    }

    /**
     * Returns the database for the given <code>databaseName</code>.
     * @param databaseName database name
     * @return database
     */
    public DatabaseDto getDatabase(final QualifiedName databaseName) {
        return databaseService.get(databaseName,
            GetDatabaseServiceParameters.builder()
                .disableOnReadMetadataIntercetor(false)
                .includeTableNames(true)
                .includeUserMetadata(true)
                .build());
    }

    /**
     * Returns the table for the given <code>tableName</code>.
     * @param tableName table name
     * @return table dto
     */
    public Optional<TableDto> getTable(final QualifiedName tableName) {
        return tableService.get(tableName, GetTableServiceParameters.builder()
            .disableOnReadMetadataIntercetor(false)
            .includeInfo(true)
            .includeDefinitionMetadata(true)
            .includeDataMetadata(true)
            .build());
    }
}
