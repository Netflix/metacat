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
import com.netflix.metacat.common.MetacatContext;
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
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.netflix.metacat.main.api.RequestWrapper.qualifyName;
import static com.netflix.metacat.main.api.RequestWrapper.requestWrapper;

public class MetacatV1Resource implements MetacatV1 {
    private final CatalogService catalogService;
    private final DatabaseService databaseService;
    private final MetacatEventBus eventBus;
    private final MViewService mViewService;
    private final TableService tableService;

    @Inject
    public MetacatV1Resource(
            CatalogService catalogService,
            DatabaseService databaseService,
            MetacatEventBus eventBus,
            MViewService mViewService,
            TableService tableService) {
        this.catalogService = catalogService;
        this.databaseService = databaseService;
        this.eventBus = eventBus;
        this.mViewService = mViewService;
        this.tableService = tableService;
    }

    @Override
    public void createCatalog(CreateCatalogDto createCatalogDto) {
        throw new MetacatNotSupportedException("Create catalog is not supported.");
    }

    @Override
    public void createDatabase(String catalogName, String databaseName,
            DatabaseCreateRequestDto databaseCreateRequestDto) {
        MetacatContext metacatContext = MetacatContextManager.getContext();
        QualifiedName name = qualifyName(() -> QualifiedName.ofDatabase(catalogName, databaseName));
        requestWrapper(name, "createDatabase", () -> {
            eventBus.postSync(new MetacatCreateDatabasePreEvent(name, metacatContext));

            DatabaseDto newDto = new DatabaseDto();
            newDto.setName(name);
            if (databaseCreateRequestDto != null) {
                newDto.setDefinitionMetadata(databaseCreateRequestDto.getDefinitionMetadata());
            }
            databaseService.create(name, newDto);

            DatabaseDto dto = databaseService.get(name, newDto.getDefinitionMetadata() != null);
            eventBus.postAsync(new MetacatCreateDatabasePostEvent(dto, metacatContext));
            return null;
        });
    }

    @Override
    public TableDto createMView(String catalogName,
            String databaseName,
            String tableName,
            String viewName,
            Boolean snapshot,
            String filter) {
        MetacatContext metacatContext = MetacatContextManager.getContext();
        QualifiedName name = qualifyName(() -> QualifiedName.ofView(catalogName, databaseName, tableName, viewName));
        return requestWrapper(name, "createMView", () -> {
            eventBus.postSync(new MetacatCreateMViewPreEvent(name, snapshot, filter, metacatContext));

            TableDto dto = mViewService.create(name);
            if (snapshot != null && snapshot) {
                mViewService.snapshotPartitions(name, filter);
            }

            eventBus.postAsync(new MetacatCreateMViewPostEvent(dto, snapshot, filter, metacatContext));
            return dto;
        });
    }

    @Override
    public TableDto createTable(String catalogName, String databaseName, String tableName, TableDto table) {
        MetacatContext metacatContext = MetacatContextManager.getContext();
        QualifiedName name = qualifyName(() -> QualifiedName.ofTable(catalogName, databaseName, tableName));
        return requestWrapper(name, "createTable", () -> {
            checkArgument(table != null, "Table cannot be null");
            checkArgument(tableName != null && !tableName.isEmpty(), "table name is required");
            checkArgument(table.getName() != null && tableName.equalsIgnoreCase(table.getName().getTableName()),
                    "Table name does not match the name in the table");

            eventBus.postSync(new MetacatCreateTablePreEvent(name, table, metacatContext));

            tableService.create(name, table);

            TableDto dto = tableService.get(name, true).orElseThrow(() -> new IllegalStateException("Should exist"));
            eventBus.postAsync(new MetacatCreateTablePostEvent(dto, metacatContext));
            return dto;
        });
    }

    @Override
    public void deleteDatabase(String catalogName, String databaseName) {
        MetacatContext metacatContext = MetacatContextManager.getContext();
        QualifiedName name = qualifyName(() -> QualifiedName.ofDatabase(catalogName, databaseName));
        requestWrapper(name, "deleteDatabase", () -> {
            DatabaseDto dto = databaseService.get(name, true);
            eventBus.postSync(new MetacatDeleteDatabasePreEvent(dto, metacatContext));

            databaseService.delete(name);

            eventBus.postAsync(new MetacatDeleteDatabasePostEvent(dto, metacatContext));
            return null;
        });
    }

    @Override
    public TableDto deleteMView(String catalogName, String databaseName, String tableName, String viewName) {
        MetacatContext metacatContext = MetacatContextManager.getContext();
        QualifiedName name = qualifyName(() -> QualifiedName.ofView(catalogName, databaseName, tableName, viewName));
        return requestWrapper(name, "deleteMView", () -> {
            eventBus.postSync(new MetacatDeleteMViewPreEvent(name, metacatContext));

            TableDto dto = mViewService.deleteAndReturn(name);

            eventBus.postAsync(new MetacatDeleteMViewPostEvent(dto, metacatContext));
            return dto;
        });
    }

    @Override
    public TableDto deleteTable(String catalogName, String databaseName, String tableName) {
        MetacatContext metacatContext = MetacatContextManager.getContext();
        QualifiedName name = qualifyName(() -> QualifiedName.ofTable(catalogName, databaseName, tableName));
        return requestWrapper(name, "deleteTable", () -> {
            eventBus.postSync(new MetacatDeleteTablePreEvent(name, metacatContext));

            TableDto dto = tableService.deleteAndReturn(name, false);

            eventBus.postAsync(new MetacatDeleteTablePostEvent(dto, metacatContext));
            return dto;
        });
    }

    @Override
    public CatalogDto getCatalog(String catalogName) {
        QualifiedName name = qualifyName(() -> QualifiedName.ofCatalog(catalogName));
        return requestWrapper(name, "getCatalog", () -> catalogService.get(name));
    }

    @Override
    public List<CatalogMappingDto> getCatalogNames() {
        QualifiedName name = QualifiedName.ofCatalog("getCatalogNames");
        return requestWrapper(name, "getCatalogNames", catalogService::getCatalogNames);
    }

    @Override
    public DatabaseDto getDatabase(String catalogName, String databaseName, Boolean includeUserMetadata) {
        QualifiedName name = qualifyName(() -> QualifiedName.ofDatabase(catalogName, databaseName));
        return requestWrapper(name, "getDatabase", () -> databaseService.get(name, includeUserMetadata));
    }

    @Override
    public TableDto getMView(String catalogName, String databaseName, String tableName, String viewName) {
        QualifiedName name = qualifyName(() -> QualifiedName.ofView(catalogName, databaseName, tableName, viewName));
        return requestWrapper(name, "getMView", (Supplier<TableDto>) () -> {
            Optional<TableDto> table = mViewService.getOpt(name);
            return table.orElseThrow(() -> new MetacatNotFoundException("Unable to find view: " + name));
        });
    }

    @Override
    public List<NameDateDto> getMViews(String catalogName) {
        QualifiedName name = qualifyName(() -> QualifiedName.ofCatalog(catalogName));
        return requestWrapper(name, "getMViews", () -> mViewService.list(name));
    }

    @Override
    public List<NameDateDto> getMViews(String catalogName, String databaseName, String tableName) {
        QualifiedName name = qualifyName(() -> QualifiedName.ofTable(catalogName, databaseName, tableName));
        return requestWrapper(name, "getMViews", () -> mViewService.list(name));
    }

    @Override
    public TableDto getTable(String catalogName, String databaseName, String tableName, Boolean includeInfo, Boolean includeDefinitionMetadata, Boolean includeDataMetadata) {
        QualifiedName name = qualifyName(() -> QualifiedName.ofTable(catalogName, databaseName, tableName));
        return requestWrapper(name, "getTable", (Supplier<TableDto>) () -> {
            Optional<TableDto> table = tableService.get(name, includeInfo, includeDefinitionMetadata, includeDataMetadata);
            return table.orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
        });
    }

    @Override
    public void renameTable(String catalogName, String databaseName, String tableName, String newTableName) {
        MetacatContext metacatContext = MetacatContextManager.getContext();
        QualifiedName oldName = qualifyName(() -> QualifiedName.ofTable(catalogName, databaseName, tableName));
        QualifiedName newName = qualifyName(() -> QualifiedName.ofTable(catalogName, databaseName, newTableName));
        requestWrapper(oldName, "renameTable", () -> {
            eventBus.postSync(new MetacatRenameTablePreEvent(newName, oldName, metacatContext));

            tableService.rename(oldName, newName, false);

            TableDto dto = tableService.get(newName, true).orElseThrow(() -> new IllegalStateException("should exist"));
            eventBus.postAsync(new MetacatRenameTablePostEvent(oldName, dto, metacatContext));
            return null;
        });
    }

    public void updateCatalog(String catalogName, CreateCatalogDto createCatalogDto) {
        MetacatContext metacatContext = MetacatContextManager.getContext();
        QualifiedName name = qualifyName(() -> QualifiedName.ofCatalog(catalogName));
        requestWrapper(name, "updateDatabase", () -> {
            eventBus.postSync(new MetacatUpdateDatabasePreEvent(name, metacatContext));

            createCatalogDto.setName(name);
            catalogService.update(name, createCatalogDto);

            eventBus.postAsync(new MetacatUpdateDatabasePostEvent(name, metacatContext));
            return null;
        });
    }

    @Override
    public void updateDatabase(
            String catalogName,
            String databaseName,
            DatabaseCreateRequestDto databaseUpdateRequestDto) {
        MetacatContext metacatContext = MetacatContextManager.getContext();
        QualifiedName name = qualifyName(() -> QualifiedName.ofDatabase(catalogName, databaseName));
        requestWrapper(name, "updateDatabase", () -> {
            eventBus.postSync(new MetacatUpdateDatabasePreEvent(name, metacatContext));

            DatabaseDto newDto = new DatabaseDto();
            newDto.setName(name);
            newDto.setDefinitionMetadata(databaseUpdateRequestDto.getDefinitionMetadata());
            databaseService.update(name, newDto);

            eventBus.postAsync(new MetacatUpdateDatabasePostEvent(name, metacatContext));
            return null;
        });
    }

    @Override
    public TableDto updateMView(String catalogName, String databaseName, String tableName, String viewName, TableDto table) {
        MetacatContext metacatContext = MetacatContextManager.getContext();
        QualifiedName name = qualifyName(() -> QualifiedName.ofView(catalogName, databaseName, tableName, viewName));
        return requestWrapper(name, "getMView", () -> {
            eventBus.postSync(new MetacatUpdateMViewPreEvent(name, table, metacatContext));

            mViewService.update(name, table);

            TableDto dto = mViewService.getOpt(name).orElseThrow(() -> new IllegalStateException("should exist"));
            eventBus.postAsync(new MetacatUpdateMViewPostEvent(dto, metacatContext));
            return dto;
        });
    }

    @Override
    public TableDto updateTable(String catalogName, String databaseName, String tableName, TableDto table) {
        MetacatContext metacatContext = MetacatContextManager.getContext();
        QualifiedName name = qualifyName(() -> QualifiedName.ofTable(catalogName, databaseName, tableName));
        return requestWrapper(name, "updateTable", () -> {
            checkArgument(table != null, "Table cannot be null");
            checkArgument(tableName != null && !tableName.isEmpty(), "table name is required");
            checkArgument(table.getName() != null && tableName.equalsIgnoreCase(table.getName().getTableName()),
                    "Table name does not match the name in the table");

            TableDto oldTable = tableService.get(name, false)
                                                 .orElseThrow(() -> new IllegalStateException(
                                                         "expect existing table to be present"));
            eventBus.postSync(new MetacatUpdateTablePreEvent(name, oldTable, table, metacatContext));

            tableService.update(name, table);

            TableDto dto = tableService.get(name, true).orElseThrow(() -> new IllegalStateException("should exist"));
            eventBus.postAsync(new MetacatUpdateTablePostEvent(dto, metacatContext));
            return dto;
        });
    }
}

