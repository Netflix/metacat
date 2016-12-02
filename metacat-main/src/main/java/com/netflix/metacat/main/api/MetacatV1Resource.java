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

import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.base.Preconditions;
import com.netflix.metacat.common.MetacatRequestContext;
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
import com.netflix.metacat.common.server.events.MetacatCreateDatabasePostEvent;
import com.netflix.metacat.common.server.events.MetacatCreateDatabasePreEvent;
import com.netflix.metacat.common.server.events.MetacatCreateMViewPostEvent;
import com.netflix.metacat.common.server.events.MetacatCreateMViewPreEvent;
import com.netflix.metacat.common.server.events.MetacatCreateTablePostEvent;
import com.netflix.metacat.common.server.events.MetacatCreateTablePreEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteDatabasePostEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteDatabasePreEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteMViewPostEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteMViewPreEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteTablePostEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteTablePreEvent;
import com.netflix.metacat.common.server.events.MetacatEventBus;
import com.netflix.metacat.common.server.events.MetacatRenameTablePostEvent;
import com.netflix.metacat.common.server.events.MetacatRenameTablePreEvent;
import com.netflix.metacat.common.server.events.MetacatUpdateDatabasePostEvent;
import com.netflix.metacat.common.server.events.MetacatUpdateDatabasePreEvent;
import com.netflix.metacat.common.server.events.MetacatUpdateMViewPostEvent;
import com.netflix.metacat.common.server.events.MetacatUpdateMViewPreEvent;
import com.netflix.metacat.common.server.events.MetacatUpdateTablePostEvent;
import com.netflix.metacat.common.server.events.MetacatUpdateTablePreEvent;
import com.netflix.metacat.common.util.MetacatContextManager;
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
    private final MetacatEventBus eventBus;
    private final MViewService mViewService;
    private final TableService tableService;

    /**
     * Constructor.
     * @param catalogService catalog service
     * @param databaseService database service
     * @param eventBus event bus
     * @param mViewService view service
     * @param tableService table service
     */
    @Inject
    public MetacatV1Resource(
        final CatalogService catalogService,
        final DatabaseService databaseService,
        final MetacatEventBus eventBus,
        final MViewService mViewService,
        final TableService tableService) {
        this.catalogService = catalogService;
        this.databaseService = databaseService;
        this.eventBus = eventBus;
        this.mViewService = mViewService;
        this.tableService = tableService;
    }

    @Override
    public void createCatalog(final CreateCatalogDto createCatalogDto) {
        throw new MetacatNotSupportedException("Create catalog is not supported.");
    }

    @Override
    public void createDatabase(final String catalogName, final String databaseName,
        final DatabaseCreateRequestDto databaseCreateRequestDto) {
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        final QualifiedName name =
            RequestWrapper.qualifyName(() -> QualifiedName.ofDatabase(catalogName, databaseName));
        RequestWrapper.requestWrapper(name, "createDatabase", () -> {
            eventBus.postSync(new MetacatCreateDatabasePreEvent(name, metacatRequestContext));

            final DatabaseDto newDto = new DatabaseDto();
            newDto.setName(name);
            if (databaseCreateRequestDto != null) {
                newDto.setDefinitionMetadata(databaseCreateRequestDto.getDefinitionMetadata());
            }
            databaseService.create(name, newDto);

            final DatabaseDto dto = databaseService.get(name, newDto.getDefinitionMetadata() != null);
            eventBus.postAsync(new MetacatCreateDatabasePostEvent(name, metacatRequestContext, dto));
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
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        final QualifiedName name =
            RequestWrapper.qualifyName(() -> QualifiedName.ofView(catalogName, databaseName, tableName, viewName));
        return RequestWrapper.requestWrapper(name, "createMView", () -> {
            eventBus.postSync(new MetacatCreateMViewPreEvent(name, metacatRequestContext, snapshot, filter));

            final TableDto dto = mViewService.create(name);
            if (snapshot != null && snapshot) {
                mViewService.snapshotPartitions(name, filter);
            }

            eventBus.postAsync(new MetacatCreateMViewPostEvent(name, metacatRequestContext, dto, snapshot, filter));
            return dto;
        });
    }

    @Override
    public TableDto createTable(final String catalogName, final String databaseName, final String tableName,
        final TableDto table) {
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        final QualifiedName name =
            RequestWrapper.qualifyName(() -> QualifiedName.ofTable(catalogName, databaseName, tableName));
        return RequestWrapper.requestWrapper(name, "createTable", () -> {
            Preconditions.checkArgument(table != null, "Table cannot be null");
            Preconditions.checkArgument(tableName != null && !tableName.isEmpty(), "table name is required");
            Preconditions.checkArgument(table.getName() != null
                    && tableName.equalsIgnoreCase(table.getName().getTableName()),
                "Table name does not match the name in the table");

            eventBus.postSync(new MetacatCreateTablePreEvent(name, metacatRequestContext, table));

            tableService.create(name, table);

            final TableDto dto =
                tableService.get(name, true).orElseThrow(() -> new IllegalStateException("Should exist"));
            eventBus.postAsync(new MetacatCreateTablePostEvent(name, metacatRequestContext, dto));
            return dto;
        });
    }

    @Override
    public void deleteDatabase(final String catalogName, final String databaseName) {
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        final QualifiedName name =
            RequestWrapper.qualifyName(() -> QualifiedName.ofDatabase(catalogName, databaseName));
        RequestWrapper.requestWrapper(name, "deleteDatabase", () -> {
            final DatabaseDto dto = databaseService.get(name, true);
            eventBus.postSync(new MetacatDeleteDatabasePreEvent(name, metacatRequestContext, dto));

            databaseService.delete(name);

            eventBus.postAsync(new MetacatDeleteDatabasePostEvent(name, metacatRequestContext, dto));
            return null;
        });
    }

    @Override
    public TableDto deleteMView(final String catalogName, final String databaseName, final String tableName,
        final String viewName) {
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        final QualifiedName name =
            RequestWrapper.qualifyName(() -> QualifiedName.ofView(catalogName, databaseName, tableName, viewName));
        return RequestWrapper.requestWrapper(name, "deleteMView", () -> {
            eventBus.postSync(new MetacatDeleteMViewPreEvent(name, metacatRequestContext));

            final TableDto dto = mViewService.deleteAndReturn(name);

            eventBus.postAsync(new MetacatDeleteMViewPostEvent(name, metacatRequestContext, dto));
            return dto;
        });
    }

    @Override
    public TableDto deleteTable(final String catalogName, final String databaseName, final String tableName) {
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        final QualifiedName name =
            RequestWrapper.qualifyName(() -> QualifiedName.ofTable(catalogName, databaseName, tableName));
        return RequestWrapper.requestWrapper(name, "deleteTable", () -> {
            eventBus.postSync(new MetacatDeleteTablePreEvent(name, metacatRequestContext));

            final TableDto dto = tableService.deleteAndReturn(name, false);

            eventBus.postAsync(new MetacatDeleteTablePostEvent(name, metacatRequestContext, dto));
            return dto;
        });
    }

    @Override
    public CatalogDto getCatalog(final String catalogName) {
        final QualifiedName name = RequestWrapper.qualifyName(() -> QualifiedName.ofCatalog(catalogName));
        return RequestWrapper.requestWrapper(name, "getCatalog", () -> catalogService.get(name));
    }

    @Override
    public List<CatalogMappingDto> getCatalogNames() {
        final QualifiedName name = QualifiedName.ofCatalog("getCatalogNames");
        return RequestWrapper.requestWrapper(name, "getCatalogNames", catalogService::getCatalogNames);
    }

    @Override
    public DatabaseDto getDatabase(final String catalogName, final String databaseName,
        final Boolean includeUserMetadata) {
        final QualifiedName name =
            RequestWrapper.qualifyName(() -> QualifiedName.ofDatabase(catalogName, databaseName));
        return RequestWrapper.requestWrapper(name, "getDatabase", () -> databaseService.get(name, includeUserMetadata));
    }

    @Override
    public TableDto getMView(final String catalogName, final String databaseName, final String tableName,
        final String viewName) {
        final QualifiedName name =
            RequestWrapper.qualifyName(() -> QualifiedName.ofView(catalogName, databaseName, tableName, viewName));
        return RequestWrapper.requestWrapper(name, "getMView", () -> {
            final Optional<TableDto> table = mViewService.getOpt(name);
            return table.orElseThrow(() -> new MetacatNotFoundException("Unable to find view: " + name));
        });
    }

    @Override
    public List<NameDateDto> getMViews(final String catalogName) {
        final QualifiedName name = RequestWrapper.qualifyName(() -> QualifiedName.ofCatalog(catalogName));
        return RequestWrapper.requestWrapper(name, "getMViews", () -> mViewService.list(name));
    }

    @Override
    public List<NameDateDto> getMViews(final String catalogName, final String databaseName, final String tableName) {
        final QualifiedName name =
            RequestWrapper.qualifyName(() -> QualifiedName.ofTable(catalogName, databaseName, tableName));
        return RequestWrapper.requestWrapper(name, "getMViews", () -> mViewService.list(name));
    }

    @Override
    public TableDto getTable(final String catalogName, final String databaseName, final String tableName,
        final Boolean includeInfo, final Boolean includeDefinitionMetadata, final Boolean includeDataMetadata) {
        final QualifiedName name =
            RequestWrapper.qualifyName(() -> QualifiedName.ofTable(catalogName, databaseName, tableName));
        return RequestWrapper.requestWrapper(name, "getTable", () -> {
            final Optional<TableDto> table = tableService
                .get(name, includeInfo, includeDefinitionMetadata, includeDataMetadata);
            return table.orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
        });
    }

    @Override
    public void renameTable(final String catalogName, final String databaseName, final String tableName,
        final String newTableName) {
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        final QualifiedName oldName =
            RequestWrapper.qualifyName(() -> QualifiedName.ofTable(catalogName, databaseName, tableName));
        final QualifiedName newName =
            RequestWrapper.qualifyName(() -> QualifiedName.ofTable(catalogName, databaseName, newTableName));
        RequestWrapper.requestWrapper(oldName, "renameTable", () -> {
            eventBus.postSync(new MetacatRenameTablePreEvent(oldName, metacatRequestContext, newName));

            final TableDto oldTable = this.tableService.get(oldName, true).orElseThrow(IllegalStateException::new);
            tableService.rename(oldName, newName, false);

            final TableDto dto =
                tableService.get(newName, true).orElseThrow(() -> new IllegalStateException("should exist"));
            eventBus.postAsync(new MetacatRenameTablePostEvent(oldName, metacatRequestContext, oldTable, dto));
            return null;
        });
    }

    @Override
    public void updateCatalog(final String catalogName, final CreateCatalogDto createCatalogDto) {
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        final QualifiedName name = RequestWrapper.qualifyName(() -> QualifiedName.ofCatalog(catalogName));
        RequestWrapper.requestWrapper(name, "updateDatabase", () -> {
            eventBus.postSync(new MetacatUpdateDatabasePreEvent(name, metacatRequestContext));

            createCatalogDto.setName(name);
            catalogService.update(name, createCatalogDto);

            eventBus.postAsync(new MetacatUpdateDatabasePostEvent(name, metacatRequestContext));
            return null;
        });
    }

    @Override
    public void updateDatabase(
        final String catalogName,
        final String databaseName,
        final DatabaseCreateRequestDto databaseUpdateRequestDto) {
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        final QualifiedName name =
            RequestWrapper.qualifyName(() -> QualifiedName.ofDatabase(catalogName, databaseName));
        RequestWrapper.requestWrapper(name, "updateDatabase", () -> {
            eventBus.postSync(new MetacatUpdateDatabasePreEvent(name, metacatRequestContext));

            final DatabaseDto newDto = new DatabaseDto();
            newDto.setName(name);
            newDto.setDefinitionMetadata(databaseUpdateRequestDto.getDefinitionMetadata());
            databaseService.update(name, newDto);

            eventBus.postAsync(new MetacatUpdateDatabasePostEvent(name, metacatRequestContext));
            return null;
        });
    }

    @Override
    public TableDto updateMView(final String catalogName, final String databaseName, final String tableName,
        final String viewName, final TableDto table) {
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        final QualifiedName name =
            RequestWrapper.qualifyName(() -> QualifiedName.ofView(catalogName, databaseName, tableName, viewName));
        return RequestWrapper.requestWrapper(name, "getMView", () -> {
            eventBus.postSync(new MetacatUpdateMViewPreEvent(name, metacatRequestContext, table));

            mViewService.update(name, table);

            final TableDto dto = mViewService.getOpt(name).orElseThrow(() -> new IllegalStateException("should exist"));
            eventBus.postAsync(new MetacatUpdateMViewPostEvent(name, metacatRequestContext, dto));
            return dto;
        });
    }

    @Override
    public TableDto updateTable(final String catalogName, final String databaseName, final String tableName,
        final TableDto table) {
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        final QualifiedName name =
            RequestWrapper.qualifyName(() -> QualifiedName.ofTable(catalogName, databaseName, tableName));
        return RequestWrapper.requestWrapper(name, "updateTable", () -> {
            Preconditions.checkArgument(table != null, "Table cannot be null");
            Preconditions.checkArgument(tableName != null && !tableName.isEmpty(), "table name is required");
            Preconditions.checkArgument(table.getName() != null
                    && tableName.equalsIgnoreCase(table.getName().getTableName()),
                "Table name does not match the name in the table");

            final TableDto oldTable = tableService.get(name, false)
                .orElseThrow(() -> new IllegalStateException(
                    "expect existing table to be present"));
            eventBus.postSync(new MetacatUpdateTablePreEvent(name, metacatRequestContext, oldTable, table));

            tableService.update(name, table);

            final TableDto dto =
                tableService.get(name, true).orElseThrow(() -> new IllegalStateException("should exist"));
            eventBus.postAsync(new MetacatUpdateTablePostEvent(name, metacatRequestContext, oldTable, dto));
            return dto;
        });
    }
}

