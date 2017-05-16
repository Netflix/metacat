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

package com.netflix.metacat.main.api;

import com.google.common.base.Preconditions;
import com.netflix.metacat.common.NameDateDto;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.api.MetacatV1;
import com.netflix.metacat.common.dto.CatalogDto;
import com.netflix.metacat.common.dto.CatalogMappingDto;
import com.netflix.metacat.common.dto.CreateCatalogDto;
import com.netflix.metacat.common.dto.DatabaseCreateRequestDto;
import com.netflix.metacat.common.dto.DatabaseDto;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.exception.MetacatNotFoundException;
import com.netflix.metacat.common.exception.MetacatNotSupportedException;
import com.netflix.metacat.common.server.connectors.exception.TableNotFoundException;
import com.netflix.metacat.main.services.CatalogService;
import com.netflix.metacat.main.services.DatabaseService;
import com.netflix.metacat.main.services.MViewService;
import com.netflix.metacat.main.services.TableService;

import javax.inject.Inject;
import java.util.List;
import java.util.Optional;

/**
 * Metacat V1 API implementation.
 */
public class MetacatV1Resource implements MetacatV1 {
    private final CatalogService catalogService;
    private final DatabaseService databaseService;
    private final MViewService mViewService;
    private final TableService tableService;
    private final RequestWrapper requestWrapper;

    /**
     * Constructor.
     * @param catalogService catalog service
     * @param databaseService database service
     * @param mViewService view service
     * @param tableService table service
     * @param requestWrapper  request wrapper object
     */
    @Inject
    public MetacatV1Resource(
        final CatalogService catalogService,
        final DatabaseService databaseService,
        final MViewService mViewService,
        final TableService tableService,
        final RequestWrapper requestWrapper) {
        this.catalogService = catalogService;
        this.databaseService = databaseService;
        this.mViewService = mViewService;
        this.tableService = tableService;
        this.requestWrapper = requestWrapper;
    }

    @Override
    public void createCatalog(final CreateCatalogDto createCatalogDto) {
        throw new MetacatNotSupportedException("Create catalog is not supported.");
    }

    @Override
    public void createDatabase(final String catalogName, final String databaseName,
        final DatabaseCreateRequestDto databaseCreateRequestDto) {
        final QualifiedName name =
            requestWrapper.qualifyName(() -> QualifiedName.ofDatabase(catalogName, databaseName));
        requestWrapper.processRequest(name, "createDatabase", () -> {
            final DatabaseDto newDto = new DatabaseDto();
            newDto.setName(name);
            if (databaseCreateRequestDto != null) {
                newDto.setDefinitionMetadata(databaseCreateRequestDto.getDefinitionMetadata());
            }
            databaseService.create(name, newDto);
            return null;
        });
    }

    @Override
    public TableDto createMView(final String catalogName,
        final String databaseName,
        final String tableName,
        final String viewName,
        final Boolean snapshot,
        final String filter
    ) {
        final QualifiedName name =
            requestWrapper.qualifyName(() -> QualifiedName.ofView(catalogName, databaseName, tableName, viewName));
        return requestWrapper.processRequest(name, "createMView",
            () -> mViewService.createAndSnapshotPartitions(name, snapshot, filter));
    }

    @Override
    public TableDto createTable(final String catalogName, final String databaseName, final String tableName,
        final TableDto table) {
        final QualifiedName name =
            requestWrapper.qualifyName(() -> QualifiedName.ofTable(catalogName, databaseName, tableName));
        return requestWrapper.processRequest(name, "createTable", () -> {
            Preconditions.checkArgument(table != null, "Table cannot be null");
            Preconditions.checkArgument(tableName != null && !tableName.isEmpty(), "table name is required");
            Preconditions.checkArgument(table.getName() != null
                    && tableName.equalsIgnoreCase(table.getName().getTableName()),
                "Table name does not match the name in the table");

            return tableService.create(name, table);
        });
    }

    @Override
    public void deleteDatabase(final String catalogName, final String databaseName) {
        final QualifiedName name =
            requestWrapper.qualifyName(() -> QualifiedName.ofDatabase(catalogName, databaseName));
        requestWrapper.processRequest(name, "deleteDatabase", () -> {
            databaseService.delete(name);
            return null;
        });
    }

    @Override
    public TableDto deleteMView(final String catalogName, final String databaseName, final String tableName,
        final String viewName) {
        final QualifiedName name =
            requestWrapper.qualifyName(() -> QualifiedName.ofView(catalogName, databaseName, tableName, viewName));
        return requestWrapper.processRequest(name, "deleteMView", () -> mViewService.deleteAndReturn(name));
    }

    @Override
    public TableDto deleteTable(final String catalogName, final String databaseName, final String tableName) {
        final QualifiedName name =
            requestWrapper.qualifyName(() -> QualifiedName.ofTable(catalogName, databaseName, tableName));
        return requestWrapper.processRequest(name, "deleteTable", () -> tableService.deleteAndReturn(name, false));
    }

    @Override
    public CatalogDto getCatalog(final String catalogName) {
        final QualifiedName name = requestWrapper.qualifyName(() -> QualifiedName.ofCatalog(catalogName));
        return requestWrapper.processRequest(name, "getCatalog", () -> catalogService.get(name));
    }

    @Override
    public List<CatalogMappingDto> getCatalogNames() {
        final QualifiedName name = QualifiedName.ofCatalog("getCatalogNames");
        return requestWrapper.processRequest(name, "getCatalogNames", catalogService::getCatalogNames);
    }

    @Override
    public DatabaseDto getDatabase(final String catalogName, final String databaseName,
        final Boolean includeUserMetadata) {
        final QualifiedName name =
            requestWrapper.qualifyName(() -> QualifiedName.ofDatabase(catalogName, databaseName));
        return requestWrapper.processRequest(name, "getDatabase", () -> databaseService.get(name, includeUserMetadata));
    }

    @Override
    public TableDto getMView(final String catalogName, final String databaseName, final String tableName,
        final String viewName) {
        final QualifiedName name =
            requestWrapper.qualifyName(() -> QualifiedName.ofView(catalogName, databaseName, tableName, viewName));
        return requestWrapper.processRequest(name, "getMView", () -> {
            final Optional<TableDto> table = mViewService.getOpt(name);
            return table.orElseThrow(() -> new MetacatNotFoundException("Unable to find view: " + name));
        });
    }

    @Override
    public List<NameDateDto> getMViews(final String catalogName) {
        final QualifiedName name = requestWrapper.qualifyName(() -> QualifiedName.ofCatalog(catalogName));
        return requestWrapper.processRequest(name, "getMViews", () -> mViewService.list(name));
    }

    @Override
    public List<NameDateDto> getMViews(final String catalogName, final String databaseName, final String tableName) {
        final QualifiedName name =
            requestWrapper.qualifyName(() -> QualifiedName.ofTable(catalogName, databaseName, tableName));
        return requestWrapper.processRequest(name, "getMViews", () -> mViewService.list(name));
    }

    @Override
    public TableDto getTable(final String catalogName, final String databaseName, final String tableName,
        final Boolean includeInfo, final Boolean includeDefinitionMetadata, final Boolean includeDataMetadata) {
        final QualifiedName name =
            requestWrapper.qualifyName(() -> QualifiedName.ofTable(catalogName, databaseName, tableName));
        return requestWrapper.processRequest(name, "getTable", () -> {
            final Optional<TableDto> table = tableService
                .get(name, includeInfo, includeDefinitionMetadata, includeDataMetadata);
            return table.orElseThrow(() -> new TableNotFoundException(name));
        });
    }

    @Override
    public void renameTable(final String catalogName, final String databaseName, final String tableName,
        final String newTableName) {
        final QualifiedName oldName =
            requestWrapper.qualifyName(() -> QualifiedName.ofTable(catalogName, databaseName, tableName));
        final QualifiedName newName =
            requestWrapper.qualifyName(() -> QualifiedName.ofTable(catalogName, databaseName, newTableName));
        requestWrapper.processRequest(oldName, "renameTable", () -> {
            tableService.rename(oldName, newName, false);
            return null;
        });
    }

    @Override
    public void updateCatalog(final String catalogName, final CreateCatalogDto createCatalogDto) {
        final QualifiedName name = requestWrapper.qualifyName(() -> QualifiedName.ofCatalog(catalogName));
        requestWrapper.processRequest(name, "updateDatabase", () -> {
            createCatalogDto.setName(name);
            catalogService.update(name, createCatalogDto);
            return null;
        });
    }

    @Override
    public void updateDatabase(
        final String catalogName,
        final String databaseName,
        final DatabaseCreateRequestDto databaseUpdateRequestDto) {
        final QualifiedName name =
            requestWrapper.qualifyName(() -> QualifiedName.ofDatabase(catalogName, databaseName));
        requestWrapper.processRequest(name, "updateDatabase", () -> {
            final DatabaseDto newDto = new DatabaseDto();
            newDto.setName(name);
            newDto.setDefinitionMetadata(databaseUpdateRequestDto.getDefinitionMetadata());
            databaseService.update(name, newDto);
            return null;
        });
    }

    @Override
    public TableDto updateMView(final String catalogName, final String databaseName, final String tableName,
        final String viewName, final TableDto table) {
        final QualifiedName name =
            requestWrapper.qualifyName(() -> QualifiedName.ofView(catalogName, databaseName, tableName, viewName));
        return requestWrapper.processRequest(name, "getMView", () -> mViewService.updateAndReturn(name, table));
    }

    @Override
    public TableDto updateTable(final String catalogName, final String databaseName, final String tableName,
        final TableDto table) {
        final QualifiedName name =
            requestWrapper.qualifyName(() -> QualifiedName.ofTable(catalogName, databaseName, tableName));
        return requestWrapper.processRequest(name, "updateTable", () -> {
            Preconditions.checkArgument(table != null, "Table cannot be null");
            Preconditions.checkArgument(tableName != null && !tableName.isEmpty(), "table name is required");
            Preconditions.checkArgument(table.getName() != null
                    && tableName.equalsIgnoreCase(table.getName().getTableName()),
                "Table name does not match the name in the table");
            return tableService.updateAndReturn(name, table);
        });
    }
}

