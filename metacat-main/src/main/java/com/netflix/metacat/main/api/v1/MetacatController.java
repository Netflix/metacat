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

package com.netflix.metacat.main.api.v1;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.netflix.metacat.common.NameDateDto;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.CatalogDto;
import com.netflix.metacat.common.dto.CatalogMappingDto;
import com.netflix.metacat.common.dto.CreateCatalogDto;
import com.netflix.metacat.common.dto.DatabaseCreateRequestDto;
import com.netflix.metacat.common.dto.DatabaseDto;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.exception.MetacatNotFoundException;
import com.netflix.metacat.common.exception.MetacatNotSupportedException;
import com.netflix.metacat.common.server.api.v1.MetacatV1;
import com.netflix.metacat.common.server.connectors.exception.TableNotFoundException;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.common.server.util.MetacatContextManager;
import com.netflix.metacat.main.api.RequestWrapper;
import com.netflix.metacat.main.services.CatalogService;
import com.netflix.metacat.main.services.DatabaseService;
import com.netflix.metacat.main.services.GetCatalogServiceParameters;
import com.netflix.metacat.main.services.GetDatabaseServiceParameters;
import com.netflix.metacat.main.services.GetTableNamesServiceParameters;
import com.netflix.metacat.main.services.GetTableServiceParameters;
import com.netflix.metacat.main.services.MViewService;
import com.netflix.metacat.main.services.MetacatServiceHelper;
import com.netflix.metacat.main.services.TableService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.DependsOn;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Nullable;

import jakarta.validation.Valid;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Metacat V1 API implementation.
 */
@RestController
@RequestMapping(
    path = "/mds/v1"
)
@Tag(
    name = "MetacatV1",
    description = "Federated metadata operations"
)
@Slf4j
@DependsOn("metacatCoreInitService")
@RequiredArgsConstructor
public class MetacatController implements MetacatV1 {
    private final CatalogService catalogService;
    private final DatabaseService databaseService;
    private final MViewService mViewService;
    private final TableService tableService;
    private final RequestWrapper requestWrapper;
    private final Config config;

    /**
     * Simple get on / to show API is up and available.
     */
    @RequestMapping(method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void index() {
        // TODO: Hypermedia
    }

    /**
     * Creates a new catalog.
     *
     * @param createCatalogDto catalog
     */
    @RequestMapping(
        method = RequestMethod.POST,
        path = "/catalog"
    )
    @ResponseStatus(HttpStatus.CREATED)
    @Operation(
        summary = "Creates a new catalog",
        description = "Returns success if there were no errors creating the catalog"
    )
    @ApiResponses(
        {
            @ApiResponse(
                responseCode = "501",
                description = "Not yet implemented"
            )
        }
    )
    public void createCatalog(@Valid @RequestBody final CreateCatalogDto createCatalogDto) {
        throw new MetacatNotSupportedException("Create catalog is not supported.");
    }

    /**
     * Creates the given database in the given catalog.
     *
     * @param catalogName              catalog name
     * @param databaseName             database name
     * @param databaseCreateRequestDto database create request
     */
    @RequestMapping(
        method = RequestMethod.POST,
        path = "/catalog/{catalog-name}/database/{database-name}"
    )
    @ResponseStatus(HttpStatus.CREATED)
    @Operation(
        summary = "Creates the given database in the given catalog",
        description = "Given a catalog and a database name, creates the database in the catalog"
    )
    @ApiResponses(
        {
            @ApiResponse(
                responseCode = "201",
                description = "The database was created"
            ),
            @ApiResponse(
                responseCode = "404",
                description = "The requested catalog or database cannot be located"
            )
        }
    )
    @Override
    public void createDatabase(
        @Parameter(description = "The name of the catalog", required = true)
        @PathVariable("catalog-name") final String catalogName,
        @Parameter(description = "The name of the database", required = true)
        @PathVariable("database-name") final String databaseName,
        @Parameter(description = "The database information")
        @Nullable @RequestBody(required = false) final DatabaseCreateRequestDto databaseCreateRequestDto
    ) {
        final QualifiedName name = this.requestWrapper.qualifyName(
            () -> QualifiedName.ofDatabase(catalogName, databaseName)
        );
        this.requestWrapper.processRequest(
            name,
            "createDatabase",
            () -> {
                final DatabaseDto newDto = new DatabaseDto();
                newDto.setName(name);
                if (databaseCreateRequestDto != null) {
                    newDto.setUri(databaseCreateRequestDto.getUri());
                    newDto.setMetadata(databaseCreateRequestDto.getMetadata());
                    newDto.setDefinitionMetadata(databaseCreateRequestDto.getDefinitionMetadata());
                }
                this.databaseService.create(name, newDto);
                return null;
            }
        );
    }

    /**
     * Creates a table.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param tableName    table name
     * @param table        TableDto with table details
     * @return created <code>TableDto</code> table
     */
    @RequestMapping(
        method = RequestMethod.POST,
        path = "/catalog/{catalog-name}/database/{database-name}/table/{table-name}"
    )
    @ResponseStatus(HttpStatus.CREATED)
    @Operation(
        summary = "Creates a table",
        description = "Creates the given table"
    )
    @ApiResponses(
        {
            @ApiResponse(
                responseCode = "201",
                description = "The table was created"
            ),
            @ApiResponse(
                responseCode = "404",
                description = "The requested catalog or database or table cannot be located"
            )
        }
    )
    @Override
    public TableDto createTable(
        @Parameter(description = "The name of the catalog", required = true)
        @PathVariable("catalog-name") final String catalogName,
        @Parameter(description = "The name of the database", required = true)
        @PathVariable("database-name") final String databaseName,
        @Parameter(description = "The name of the table", required = true)
        @PathVariable("table-name") final String tableName,
        @Parameter(description = "The table information", required = true)
        @Valid @RequestBody final TableDto table
    ) {
        final QualifiedName name = this.requestWrapper.qualifyName(
            () -> QualifiedName.ofTable(catalogName, databaseName, tableName)
        );
        if (MetacatServiceHelper.isIcebergTable(table)) {
            MetacatContextManager.getContext().updateTableTypeMap(name, MetacatServiceHelper.ICEBERG_TABLE_TYPE);
        }

        log.info("Creating table: {} with info: {}", name, table);

        return this.requestWrapper.processRequest(
            name,
            "createTable",
            () -> {
                Preconditions.checkArgument(
                    table.getName() != null
                        && tableName.equalsIgnoreCase(table.getName().getTableName()
                    ),
                    "Table name does not match the name in the table"
                );

                return this.tableService.create(name, table);
            }
        );
    }

    /**
     * Creates a metacat view. A staging table that can contain partitions referring to the table partition locations.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param tableName    table name
     * @param viewName     view name
     * @param snapshot     boolean to snapshot or not
     * @param filter       filter expression to use
     * @return created <code>TableDto</code> mview
     */
    @RequestMapping(
        method = RequestMethod.POST,
        path = "/catalog/{catalog-name}/database/{database-name}/table/{table-name}/mview/{view-name}"
    )
    @ResponseStatus(HttpStatus.CREATED)
    @Operation(
        summary = "Creates a metacat view. A staging table that can contain partitions referring to the table partition"
            + " locations.",
        description = "Creates the given metacat view. A staging table that can contain partitions referring to the "
            + "table partition locations."
    )
    @ApiResponses(
        {
            @ApiResponse(
                responseCode = "201",
                description = "The mView was created"
            ),
            @ApiResponse(
                responseCode = "404",
                description = "The requested catalog or database or table cannot be located"
            )
        }
    )
    public TableDto createMView(
        @Parameter(description = "The name of the catalog", required = true)
        @PathVariable("catalog-name") final String catalogName,
        @Parameter(description = "The name of the database", required = true)
        @PathVariable("database-name") final String databaseName,
        @Parameter(description = "The name of the table", required = true)
        @PathVariable("table-name") final String tableName,
        @Parameter(description = "The name of the view", required = true)
        @PathVariable("view-name") final String viewName,
        @Parameter(
            description = "To snapshot a list of partitions of the table to this view. "
                + "If true, it will restore the partitions from the table to this view."
        )
        @RequestParam(name = "snapshot", defaultValue = "false") final boolean snapshot,
        @Parameter(description = "Filter expression string to use")
        @Nullable @RequestParam(value = "filter", required = false) final String filter
    ) {
        final QualifiedName name = this.requestWrapper.qualifyName(
            () -> QualifiedName.ofView(catalogName, databaseName, tableName, viewName)
        );
        return this.requestWrapper.processRequest(
            name,
            "createMView",
            () -> this.mViewService.createAndSnapshotPartitions(name, snapshot, filter)
        );
    }


    /**
     * Deletes the given database from the given catalog.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     */
    @RequestMapping(method = RequestMethod.DELETE, path = "/catalog/{catalog-name}/database/{database-name}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @Operation(
        summary = "Deletes the given database from the given catalog",
        description = "Given a catalog and database, deletes the database from the catalog"
    )
    @ApiResponses(
        {
            @ApiResponse(
                responseCode = "200",
                description = "Database was successfully deleted"
            ),
            @ApiResponse(
                responseCode = "404",
                description = "The requested catalog or database cannot be located"
            )
        }
    )
    public void deleteDatabase(
        @Parameter(description = "The name of the catalog", required = true)
        @PathVariable("catalog-name") final String catalogName,
        @Parameter(description = "The name of the database", required = true)
        @PathVariable("database-name") final String databaseName
    ) {
        final QualifiedName name = this.requestWrapper.qualifyName(
            () -> QualifiedName.ofDatabase(catalogName, databaseName)
        );
        this.requestWrapper.processRequest(
            name,
            "deleteDatabase",
            () -> {
                this.databaseService.delete(name);
                return null;
            }
        );
    }

    /**
     * Delete table.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param tableName    table name
     * @return deleted <code>TableDto</code> table.
     */
    @RequestMapping(
        method = RequestMethod.DELETE,
        path = "/catalog/{catalog-name}/database/{database-name}/table/{table-name}"
    )
    @ResponseStatus(HttpStatus.OK)
    @Operation(
        summary = "Delete table",
        description = "Deletes the given table"
    )
    @ApiResponses(
        {
            @ApiResponse(
                responseCode = "200",
                description = "Table was successfully deleted"
            ),
            @ApiResponse(
                responseCode = "404",
                description = "The requested catalog or database or table cannot be located"
            )
        }
    )
    @Override
    public TableDto deleteTable(
        @Parameter(description = "The name of the catalog", required = true)
        @PathVariable("catalog-name") final String catalogName,
        @Parameter(description = "The name of the database", required = true)
        @PathVariable("database-name") final String databaseName,
        @Parameter(description = "The name of the table", required = true)
        @PathVariable("table-name") final String tableName
    ) {
        final QualifiedName name = this.requestWrapper.qualifyName(
            () -> QualifiedName.ofTable(catalogName, databaseName, tableName)
        );
        return this.requestWrapper.processRequest(
            name,
            "deleteTable",
            () -> this.tableService.deleteAndReturn(name, false)
        );
    }

    /**
     * Delete metacat view.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param tableName    table name
     * @param viewName     view name
     * @return deleted <code>TableDto</code> mview.
     */
    @RequestMapping(
        method = RequestMethod.DELETE,
        path = "/catalog/{catalog-name}/database/{database-name}/table/{table-name}/mview/{view-name}"
    )
    @ResponseStatus(HttpStatus.OK)
    @Operation(
        summary = "Delete metacat view",
        description = "Deletes the given metacat view"
    )
    @ApiResponses(
        {
            @ApiResponse(
                responseCode = "200",
                description = "View was successfully deleted"
            ),
            @ApiResponse(
                responseCode = "404",
                description = "The requested catalog or database or metacat view cannot be located"
            )
        }
    )
    public TableDto deleteMView(
        @Parameter(description = "The name of the catalog", required = true)
        @PathVariable("catalog-name") final String catalogName,
        @Parameter(description = "The name of the database", required = true)
        @PathVariable("database-name") final String databaseName,
        @Parameter(description = "The name of the table", required = true)
        @PathVariable("table-name") final String tableName,
        @Parameter(description = "The name of the metacat view", required = true)
        @PathVariable("view-name") final String viewName
    ) {
        final QualifiedName name = this.requestWrapper.qualifyName(
            () -> QualifiedName.ofView(catalogName, databaseName, tableName, viewName)
        );
        return this.requestWrapper.processRequest(
            name,
            "deleteMView",
            () -> this.mViewService.deleteAndReturn(name)
        );
    }

    @Override
    public CatalogDto getCatalog(final String catalogName) {
        return getCatalog(catalogName, true, true);
    }

    /**
     * Get the catalog by name.
     *
     * @param catalogName catalog name
     * @return catalog
     */
    @RequestMapping(method = RequestMethod.GET, path = "/catalog/{catalog-name}")
    @ResponseStatus(HttpStatus.OK)
    @Operation(
        summary = "Databases for the requested catalog",
        description = "The list of databases that belong to the given catalog"
    )
    @ApiResponses(
        {
            @ApiResponse(
                responseCode = "200",
                description = "The catalog is returned"
            ),
            @ApiResponse(
                responseCode = "404",
                description = "The requested catalog cannot be located"
            )
        }
    )
    @Override
    public CatalogDto getCatalog(
        @Parameter(description = "The name of the catalog", required = true)
        @PathVariable("catalog-name") final String catalogName,
        @Parameter(description = "Whether to include list of database names")
        @Nullable @RequestParam(name = "includeDatabaseNames", required = false) final Boolean includeDatabaseNames,
        @Parameter(description = "Whether to include user metadata information to the response")
        @RequestParam(name = "includeUserMetadata", defaultValue = "true") final boolean includeUserMetadata) {
        final QualifiedName name = this.requestWrapper.qualifyName(() -> QualifiedName.ofCatalog(catalogName));
        final boolean includeDatabaseNamesEnable = includeDatabaseNames == null
            ? config.listDatabaseNameByDefaultOnGetCatalog() : includeDatabaseNames;

        return this.requestWrapper.processRequest(
            name,
            "getCatalog",
            Collections.singletonMap("includeDatabaseNames", String.valueOf(includeDatabaseNamesEnable)),
            () -> this.catalogService.get(name, GetCatalogServiceParameters.builder()
                .includeDatabaseNames(includeDatabaseNames == null
                    ? config.listDatabaseNameByDefaultOnGetCatalog() : includeDatabaseNames)
                .includeUserMetadata(includeUserMetadata).build())
        );
    }

    /**
     * List registered catalogs.
     *
     * @return registered catalogs.
     */
    @RequestMapping(method = RequestMethod.GET, path = "/catalog")
    @ResponseStatus(HttpStatus.OK)
    @Operation(
        summary = "List registered catalogs",
        description = "The names and types of all catalogs registered with this server"
    )
    @ApiResponses(
        {
            @ApiResponse(
                responseCode = "200",
                description = "The catalogs are returned"
            ),
            @ApiResponse(
                responseCode = "404",
                description = "No catalogs are registered with the server"
            )
        }
    )
    public List<CatalogMappingDto> getCatalogNames() {
        final QualifiedName name = QualifiedName.ofCatalog("getCatalogNames");
        return this.requestWrapper.processRequest(
            name,
            "getCatalogNames",
            this.catalogService::getCatalogNames);
    }

    /**
     * Get the database with the list of table names under it.
     *
     * @param catalogName         catalog name
     * @param databaseName        database name
     * @param includeUserMetadata true if details should include user metadata
     * @return database with details
     */
    @RequestMapping(method = RequestMethod.GET, path = "/catalog/{catalog-name}/database/{database-name}")
    @ResponseStatus(HttpStatus.OK)
    @Operation(
        summary = "Tables for the requested database",
        description = "The list of tables that belong to the given catalog and database"
    )
    @ApiResponses(
        {
            @ApiResponse(
                responseCode = "200",
                description = "The database is returned"
            ),
            @ApiResponse(
                responseCode = "404",
                description = "The requested catalog or database cannot be located"
            )
        }
    )
    @Override
    public DatabaseDto getDatabase(
        @Parameter(description = "The name of the catalog", required = true)
        @PathVariable("catalog-name") final String catalogName,
        @Parameter(description = "The name of the database", required = true)
        @PathVariable("database-name") final String databaseName,
        @Parameter(description = "Whether to include user metadata information to the response")
        @RequestParam(name = "includeUserMetadata", defaultValue = "true") final boolean includeUserMetadata,
        @Parameter(description = "Whether to include list of table names")
        @Nullable @RequestParam(name = "includeTableNames", required = false) final Boolean includeTableNames
    ) {
        final QualifiedName name = this.requestWrapper.qualifyName(
            () -> QualifiedName.ofDatabase(catalogName, databaseName)
        );
        return this.requestWrapper.processRequest(
            name,
            "getDatabase",
            Collections.singletonMap("includeTableNamesPassed", includeTableNames == null ? "false" : "true"),
            () -> databaseService.get(name,
                GetDatabaseServiceParameters.builder()
                    .includeUserMetadata(includeUserMetadata)
                    .includeTableNames(includeTableNames == null
                        ? config.listTableNamesByDefaultOnGetDatabase() : includeTableNames)
                    .disableOnReadMetadataIntercetor(false)
                    .build())
        );
    }

    /**
     * Get the table.
     *
     * @param catalogName               catalog name
     * @param databaseName              database name
     * @param tableName                 table name.
     * @param includeInfo               true if the details need to be included
     * @param includeDefinitionMetadata true if the definition metadata to be included
     * @param includeDataMetadata       true if the data metadata to be included
     * @return table
     */
    @RequestMapping(
        method = RequestMethod.GET,
        path = "/catalog/{catalog-name}/database/{database-name}/table/{table-name}"
    )
    @Operation(
        summary = "Table information",
        description = "Table information for the given table name under the given catalog and database")
    @ApiResponses(
        {
            @ApiResponse(
                responseCode = "200",
                description = "The table is returned"
            ),
            @ApiResponse(
                responseCode = "404",
                description = "The requested catalog or database or table cannot be located"
            )
        }
    )
    @Override
    public TableDto getTable(
        @Parameter(description = "The name of the catalog", required = true)
        @PathVariable("catalog-name") final String catalogName,
        @Parameter(description = "The name of the database", required = true)
        @PathVariable("database-name") final String databaseName,
        @Parameter(description = "The name of the table", required = true)
        @PathVariable("table-name") final String tableName,
        @Parameter(
            description = "Whether to include the core information about the table (location, serde, columns) in "
                + "the response. You would only say false here if you only want metadata."
        )
        @RequestParam(name = "includeInfo", defaultValue = "true") final boolean includeInfo,
        @Parameter(description = "Whether to include user definition metadata information to the response")
        @RequestParam(
            name = "includeDefinitionMetadata",
            defaultValue = "true"
        ) final boolean includeDefinitionMetadata,
        @Parameter(description = "Whether to include user data metadata information to the response")
        @RequestParam(name = "includeDataMetadata", defaultValue = "true") final boolean includeDataMetadata,
        @Parameter(description = "Whether to include more info details to the response. This value is considered only "
            + "if includeInfo is true.")
        @RequestParam(name = "includeInfoDetails", defaultValue = "false") final boolean includeInfoDetails,
        @Parameter(description = "Whether to include only the metadata location in the response")
        @RequestParam(
            name = "includeMetadataLocationOnly",
            defaultValue = "false") final boolean includeMetadataLocationOnly
    ) {
        final Supplier<QualifiedName> qualifiedNameSupplier =
            () -> QualifiedName.ofTable(catalogName, databaseName, tableName);
        final QualifiedName name = this.requestWrapper.qualifyName(qualifiedNameSupplier);
        return this.requestWrapper.processRequest(
            name,
            "getTable",
            ImmutableMap.<String, String>builder()
                .put("includeInfo", String.valueOf(includeInfo))
                .put("includeDefinitionMetadata", String.valueOf(includeDefinitionMetadata))
                .put("includeDataMetadata", String.valueOf(includeDataMetadata))
                .put("includeMetadataFromConnector", String.valueOf(includeInfoDetails))
                .put("includeMetadataLocationOnly", String.valueOf(includeMetadataLocationOnly))
                .build(),
            () -> {
                final Optional<TableDto> table = this.tableService.get(
                    name,
                    GetTableServiceParameters.builder()
                        .includeInfo(includeInfo)
                        .includeDefinitionMetadata(includeDefinitionMetadata)
                        .includeDataMetadata(includeDataMetadata)
                        .disableOnReadMetadataIntercetor(false)
                        .includeMetadataFromConnector(includeInfoDetails)
                        .includeMetadataLocationOnly(includeMetadataLocationOnly)
                        .useCache(true)
                        .build()
                );

                final TableDto tableDto = table.orElseThrow(() -> new TableNotFoundException(name));
                // Set the name to whatever the request was for because
                // for aliases, this could've been set to the original name
                tableDto.setName(qualifiedNameSupplier.get());
                return tableDto;
            }
        );
    }

    /**
     * Check if the table exists.
     *
     * @param catalogName               catalog name
     * @param databaseName              database name
     * @param tableName                 table name.
     */
    @RequestMapping(
        method = RequestMethod.HEAD,
        path = "/catalog/{catalog-name}/database/{database-name}/table/{table-name}"
    )
    @Operation(
        summary = "Table information",
        description = "Table information for the given table name under the given catalog and database")
    @ApiResponses(
        {
            @ApiResponse(
                responseCode = "200",
                description = "Table exists"
            ),
            @ApiResponse(
                responseCode = "404",
                description = "Table does not exists"
            )
        }
    )
    @Override
    public void tableExists(@Parameter(description = "The name of the catalog", required = true)
                            @PathVariable("catalog-name") final String catalogName,
                            @Parameter(description = "The name of the database", required = true)
                            @PathVariable("database-name") final String databaseName,
                            @Parameter(description = "The name of the table", required = true)
                            @PathVariable("table-name") final String tableName) {
        final Supplier<QualifiedName> qualifiedNameSupplier =
            () -> QualifiedName.ofTable(catalogName, databaseName, tableName);
        final QualifiedName name = this.requestWrapper.qualifyName(qualifiedNameSupplier);
        this.requestWrapper.processRequest(
            name,
            "exists",
            () -> {
                if (!tableService.exists(name)) {
                    throw new TableNotFoundException(name);
                }
                return null;
            }
        );
    }

    @RequestMapping(
        method = RequestMethod.GET,
        path = "/catalog/{catalog-name}/table-names"
    )
    @Operation(
        summary = "Filtered list of table names",
        description = "Filtered list of table names for the given catalog. The filter expression pattern depends on "
            + "the catalog")
    @ApiResponses(
        {
            @ApiResponse(
                responseCode = "200",
                description = "List of table names is returned"
            ),
            @ApiResponse(
                responseCode = "404",
                description = "The requested catalog cannot be located"
            )
        }
    )
    @Override
    public List<QualifiedName> getTableNames(
        @Parameter(description = "The name of the catalog", required = true)
        @PathVariable("catalog-name") final String catalogName,
        @Parameter(description = "filter expression")
        @RequestParam(name = "filter") final String filter,
        @Parameter(description = "Size of the list")
        @Nullable @RequestParam(name = "limit", required = false, defaultValue = "-1") final Integer limit) {
        final Supplier<QualifiedName> qualifiedNameSupplier =
            () -> QualifiedName.ofCatalog(catalogName);
        final QualifiedName name = this.requestWrapper.qualifyName(qualifiedNameSupplier);
        return this.requestWrapper.processRequest(
            name,
            "getTableNames",
            () -> {
                return this.tableService.getQualifiedNames(
                    name,
                    GetTableNamesServiceParameters.builder()
                        .filter(filter)
                        .limit(limit)
                        .build()
                );
            }
        );
    }

    @RequestMapping(
        method = RequestMethod.GET,
        path = "/catalog/{catalog-name}/database/{database-name}/table-names"
    )
    @Operation(
        summary = "Filtered list of table names",
        description = "Filtered list of table names for the given database. The filter expression pattern depends on "
            + "the catalog")
    @ApiResponses(
        {
            @ApiResponse(
                responseCode = "200",
                description = "List of table names is returned"
            ),
            @ApiResponse(
                responseCode = "404",
                description = "The requested catalog cannot be located"
            )
        }
    )
    @Override
    public List<QualifiedName> getTableNames(
        @Parameter(description = "The name of the catalog", required = true)
        @PathVariable("catalog-name") final String catalogName,
        @Parameter(description = "The name of the database", required = true)
        @PathVariable("database-name") final String databaseName,
        @Parameter(description = "filter expression")
        @RequestParam(name = "filter") final String filter,
        @Parameter(description = "Size of the list")
        @Nullable @RequestParam(name = "limit", required = false, defaultValue = "-1") final Integer limit) {
        final Supplier<QualifiedName> qualifiedNameSupplier =
            () -> QualifiedName.ofDatabase(catalogName, databaseName);
        final QualifiedName name = this.requestWrapper.qualifyName(qualifiedNameSupplier);
        return this.requestWrapper.processRequest(
            name,
            "getTableNames",
            () -> {
                return this.tableService.getQualifiedNames(
                    name,
                    GetTableNamesServiceParameters.builder()
                        .filter(filter)
                        .limit(limit)
                        .build()
                );
            }
        );
    }

    /**
     * List of metacat view names.
     *
     * @param catalogName catalog name
     * @return list of metacat view names.
     */
    @RequestMapping(method = RequestMethod.GET, path = "/catalog/{catalog-name}/mviews")
    @ResponseStatus(HttpStatus.OK)
    @Operation(
        summary = "List of metacat views",
        description = "List of metacat views for a catalog"
    )
    @ApiResponses(
        {
            @ApiResponse(
                responseCode = "200",
                description = "The list of views is returned"
            ),
            @ApiResponse(
                responseCode = "404",
                description = "The requested catalog cannot be located"
            )
        }
    )
    public List<NameDateDto> getMViews(
        @Parameter(description = "The name of the catalog", required = true)
        @PathVariable("catalog-name") final String catalogName
    ) {
        final QualifiedName name = this.requestWrapper.qualifyName(() -> QualifiedName.ofCatalog(catalogName));
        return this.requestWrapper.processRequest(
            name,
            "getMViews",
            () -> mViewService.list(name)
        );
    }

    /**
     * List of metacat view names.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param tableName    table name
     * @return List of metacat view names.
     */
    @RequestMapping(
        method = RequestMethod.GET,
        path = "/catalog/{catalog-name}/database/{database-name}/table/{table-name}/mviews"
    )
    @ResponseStatus(HttpStatus.OK)
    @Operation(
        summary = "List of metacat views",
        description = "List of metacat views for a catalog"
    )
    @ApiResponses(
        {
            @ApiResponse(
                responseCode = "200",
                description = "The list of views is returned"
            ),
            @ApiResponse(
                responseCode = "404",
                description = "The requested catalog cannot be located"
            )
        }
    )
    public List<NameDateDto> getMViews(
        @Parameter(description = "The name of the catalog", required = true)
        @PathVariable("catalog-name") final String catalogName,
        @Parameter(description = "The name of the database", required = true)
        @PathVariable("database-name") final String databaseName,
        @Parameter(description = "The name of the table", required = true)
        @PathVariable("table-name") final String tableName
    ) {
        final QualifiedName name = this.requestWrapper.qualifyName(
            () -> QualifiedName.ofTable(catalogName, databaseName, tableName)
        );
        return this.requestWrapper.processRequest(
            name,
            "getMViews",
            () -> this.mViewService.list(name)
        );
    }

    /**
     * Get metacat view.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param tableName    table name
     * @param viewName     view name
     * @return metacat view
     */
    @RequestMapping(
        method = RequestMethod.GET,
        path = "/catalog/{catalog-name}/database/{database-name}/table/{table-name}/mview/{view-name}"
    )
    @ResponseStatus(HttpStatus.OK)
    @Operation(
        summary = "Metacat View information",
        description = "View information for the given view name under the given catalog and database"
    )
    @ApiResponses(
        {
            @ApiResponse(
                responseCode = "200",
                description = "The view is returned"
            ),
            @ApiResponse(
                responseCode = "404",
                description = "The requested catalog or database or table cannot be located"
            )
        }
    )
    public TableDto getMView(
        @Parameter(description = "The name of the catalog", required = true)
        @PathVariable("catalog-name") final String catalogName,
        @Parameter(description = "The name of the database", required = true)
        @PathVariable("database-name") final String databaseName,
        @Parameter(description = "The name of the table", required = true)
        @PathVariable("table-name") final String tableName,
        @Parameter(description = "The name of the view", required = true)
        @PathVariable("view-name") final String viewName
    ) {
        final QualifiedName name = this.requestWrapper.qualifyName(
            () -> QualifiedName.ofView(catalogName, databaseName, tableName, viewName)
        );
        return this.requestWrapper.processRequest(
            name,
            "getMView",
            () -> {
                final Optional<TableDto> table = this.mViewService.getOpt(name,
                    GetTableServiceParameters.builder()
                        .includeDataMetadata(true)
                        .includeDefinitionMetadata(true)
                        .includeInfo(true)
                        .disableOnReadMetadataIntercetor(false)
                        .build());
                return table.orElseThrow(() -> new MetacatNotFoundException("Unable to find view: " + name));
            }
        );
    }

    /**
     * Rename table.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param tableName    table name
     * @param newTableName new table name
     */
    @RequestMapping(
        method = RequestMethod.POST,
        path = "/catalog/{catalog-name}/database/{database-name}/table/{table-name}/rename"
    )
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @Operation(
        summary = "Rename table",
        description = "Renames the given table with the new name")
    @ApiResponses(
        {
            @ApiResponse(
                responseCode = "200",
                description = "Table successfully renamed"
            ),
            @ApiResponse(
                responseCode = "404",
                description = "The requested catalog or database or table cannot be located"
            )
        }
    )
    @Override
    public void renameTable(
        @Parameter(description = "The name of the catalog", required = true)
        @PathVariable("catalog-name") final String catalogName,
        @Parameter(description = "The name of the database", required = true)
        @PathVariable("database-name") final String databaseName,
        @Parameter(description = "The name of the table", required = true)
        @PathVariable("table-name") final String tableName,
        @Parameter(description = "The name of the table", required = true)
        @RequestParam("newTableName") final String newTableName
    ) {
        final QualifiedName oldName = this.requestWrapper.qualifyName(
            () -> QualifiedName.ofTable(catalogName, databaseName, tableName)
        );
        final QualifiedName newName = this.requestWrapper.qualifyName(
            () -> QualifiedName.ofTable(catalogName, databaseName, newTableName)
        );
        this.requestWrapper.processRequest(
            oldName,
            "renameTable",
            () -> {
                this.tableService.rename(oldName, newName, false);
                return null;
            }
        );
    }

    /**
     * Updates an existing catalog.
     *
     * @param catalogName      catalog name
     * @param createCatalogDto catalog
     */
    @RequestMapping(
        method = RequestMethod.PUT,
        path = "/catalog/{catalog-name}"
    )
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @Operation(
        summary = "Updates an existing catalog",
        description = "Returns success if there were no errors updating the catalog"
    )
    @ApiResponses(
        {
            @ApiResponse(
                responseCode = "200",
                description = "Catalog successfully updated"
            ),
            @ApiResponse(
                responseCode = "404",
                description = "No catalogs are registered with the server"
            )
        }
    )
    public void updateCatalog(
        @Parameter(description = "The name of the catalog", required = true)
        @PathVariable("catalog-name") final String catalogName,
        @Parameter(description = "The metadata to update in the catalog", required = true)
        @RequestBody final CreateCatalogDto createCatalogDto
    ) {
        final QualifiedName name = this.requestWrapper.qualifyName(() -> QualifiedName.ofCatalog(catalogName));
        this.requestWrapper.processRequest(
            name,
            "updateCatalog",
            () -> {
                createCatalogDto.setName(name);
                this.catalogService.update(name, createCatalogDto);
                return null;
            }
        );
    }

    /**
     * Updates the given database in the given catalog.
     *
     * @param catalogName              catalog name.
     * @param databaseName             database name.
     * @param databaseUpdateRequestDto database
     */
    @RequestMapping(
        method = RequestMethod.PUT,
        path = "/catalog/{catalog-name}/database/{database-name}"
    )
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @Operation(
        summary = "Updates the given database in the given catalog",
        description = "Given a catalog and a database name, updates the database in the catalog"
    )
    @ApiResponses(
        {
            @ApiResponse(
                responseCode = "200",
                description = "Database successfully updated"
            ),
            @ApiResponse(
                responseCode = "404",
                description = "The requested catalog or database cannot be located"
            )
        }
    )
    public void updateDatabase(
        @Parameter(description = "The name of the catalog", required = true)
        @PathVariable("catalog-name") final String catalogName,
        @Parameter(description = "The name of the database", required = true)
        @PathVariable("database-name") final String databaseName,
        @Parameter(description = "The database information", required = true)
        @RequestBody final DatabaseCreateRequestDto databaseUpdateRequestDto
    ) {
        final QualifiedName name = this.requestWrapper.qualifyName(
            () -> QualifiedName.ofDatabase(catalogName, databaseName)
        );
        this.requestWrapper.processRequest(
            name,
            "updateDatabase",
            () -> {
                final DatabaseDto newDto = new DatabaseDto();
                newDto.setName(name);
                newDto.setUri(databaseUpdateRequestDto.getUri());
                newDto.setMetadata(databaseUpdateRequestDto.getMetadata());
                newDto.setDefinitionMetadata(databaseUpdateRequestDto.getDefinitionMetadata());
                this.databaseService.update(name, newDto);
                return null;
            }
        );
    }

    /**
     * Update metacat view.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param tableName    table name
     * @param viewName     view name
     * @param table        view
     * @return updated metacat view
     */
    @RequestMapping(
        method = RequestMethod.PUT,
        path = "/catalog/{catalog-name}/database/{database-name}/table/{table-name}/mview/{view-name}"
    )
    @ResponseStatus(HttpStatus.OK)
    @Operation(
        summary = "Update mview",
        description = "Updates the given mview"
    )
    @ApiResponses(
        {
            @ApiResponse(
                responseCode = "200",
                description = "View successfully updated"
            ),
            @ApiResponse(
                responseCode = "404",
                description = "The requested catalog or database or table cannot be located"
            )
        }
    )
    public TableDto updateMView(
        @Parameter(description = "The name of the catalog", required = true)
        @PathVariable("catalog-name") final String catalogName,
        @Parameter(description = "The name of the database", required = true)
        @PathVariable("database-name") final String databaseName,
        @Parameter(description = "The name of the table", required = true)
        @PathVariable("table-name") final String tableName,
        @Parameter(description = "The name of the view", required = true)
        @PathVariable("view-name") final String viewName,
        @Parameter(description = "The view information", required = true)
        @RequestBody final TableDto table
    ) {
        final QualifiedName name = this.requestWrapper.qualifyName(
            () -> QualifiedName.ofView(catalogName, databaseName, tableName, viewName)
        );
        return this.requestWrapper.processRequest(
            name,
            "getMView",
            () -> this.mViewService.updateAndReturn(name, table)
        );
    }

    /**
     * Update table.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param tableName    table name
     * @param table        table
     * @return table
     */
    @RequestMapping(
        method = RequestMethod.PUT,
        path = "/catalog/{catalog-name}/database/{database-name}/table/{table-name}"
    )
    @Operation(
        summary = "Update table",
        description = "Updates the given table"
    )
    @ApiResponses(
        {
            @ApiResponse(
                responseCode = "200",
                description = "Table successfully updated"
            ),
            @ApiResponse(
                responseCode = "404",
                description = "The requested catalog or database or table cannot be located"
            )
        }
    )
    @Override
    public TableDto updateTable(
        @Parameter(description = "The name of the catalog", required = true)
        @PathVariable("catalog-name") final String catalogName,
        @Parameter(description = "The name of the database", required = true)
        @PathVariable("database-name") final String databaseName,
        @Parameter(description = "The name of the table", required = true)
        @PathVariable("table-name") final String tableName,
        @Parameter(description = "The table information", required = true)
        @RequestBody final TableDto table,
        @RequestParam(
            name = "shouldThrowExceptionOnMetadataSaveFailure",
            defaultValue = "false") final boolean shouldThrowExceptionOnMetadataSaveFailure
    ) {
        final QualifiedName name = this.requestWrapper.qualifyName(
            () -> QualifiedName.ofTable(catalogName, databaseName, tableName)
        );
        return this.requestWrapper.processRequest(
            name,
            "updateTable",
            () -> {
                Preconditions.checkArgument(table.getName() != null
                        && tableName.equalsIgnoreCase(table.getName().getTableName()
                    ),
                    "Table name does not match the name in the table"
                );
                return this.tableService.updateAndReturn(name, table, shouldThrowExceptionOnMetadataSaveFailure);
            }
        );
    }
}
