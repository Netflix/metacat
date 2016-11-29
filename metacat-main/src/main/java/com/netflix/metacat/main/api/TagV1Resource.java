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
import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.api.TagV1;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.server.events.MetacatEventBus;
import com.netflix.metacat.common.server.events.MetacatUpdateTablePostEvent;
import com.netflix.metacat.common.usermetadata.TagService;
import com.netflix.metacat.common.util.MetacatContextManager;
import com.netflix.metacat.main.services.TableService;

import javax.inject.Inject;
import java.util.List;
import java.util.Set;

import static com.netflix.metacat.main.api.RequestWrapper.qualifyName;
import static com.netflix.metacat.main.api.RequestWrapper.requestWrapper;

/**
 * @author amajumdar
 */
public class TagV1Resource implements TagV1 {
    private TagService tagService;
    private MetacatEventBus eventBus;
    private TableService tableService;

    @Inject
    public TagV1Resource(MetacatEventBus eventBus, TagService tagService, TableService tableService) {
        this.tagService = tagService;
        this.eventBus = eventBus;
        this.tableService = tableService;
    }

    @Override
    public Set<String> getTags() {
        return requestWrapper("TagV1Resource.getTags", tagService::getTags);
    }

    @Override
    public List<QualifiedName> list(
            Set<String> includeTags,
            Set<String> excludeTags,
            String sourceName,
            String databaseName,
            String tableName) {
        return requestWrapper("TagV1Resource.list", () -> tagService.list(includeTags, excludeTags, sourceName, databaseName, tableName));
    }

    @Override
    public List<QualifiedName> search(
            String tag,
            String sourceName,
            String databaseName,
            String tableName) {
        return requestWrapper("TagV1Resource.search", () -> tagService.search(tag, sourceName, databaseName, tableName));
    }

    @Override
    public Set<String> setTableTags(
            String catalogName,
            String databaseName,
            String tableName,
            Set<String> tags) {
        MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        QualifiedName name = qualifyName(() -> QualifiedName.ofTable(catalogName, databaseName, tableName));
        return requestWrapper(name, "TagV1Resource.setTableTags", () -> {
            if (tableService.exists(name)) {
                throw new TableNotFoundException(new SchemaTableName(name.getDatabaseName(), name.getTableName()));
            }
            TableDto oldTable = this.tableService
                    .get(name, true)
                    .orElseThrow(IllegalStateException::new);
            Set<String> result = tagService.setTableTags(name, tags, true);
            TableDto currentTable = this.tableService
                    .get(name, true)
                    .orElseThrow(IllegalStateException::new);
            eventBus.postAsync(new MetacatUpdateTablePostEvent(name, metacatRequestContext, oldTable, currentTable));
            return result;
        });
    }

    @Override
    public void removeTableTags(
            String catalogName,
            String databaseName,
            String tableName,
            Boolean deleteAll,
            Set<String> tags) {
        MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        QualifiedName name = qualifyName(() -> QualifiedName.ofTable(catalogName, databaseName, tableName));
        requestWrapper(name, "TagV1Resource.removeTableTags", () -> {
            if (!tableService.exists(name)) {
                // Delete tags if exists
                tagService.delete(name, false);
                throw new TableNotFoundException(new SchemaTableName(name.getDatabaseName(), name.getTableName()));
            }
            TableDto oldTable = this.tableService
                    .get(name, true)
                    .orElseThrow(IllegalStateException::new);
            tagService.removeTableTags(name, deleteAll, tags, true);
            TableDto currentTable = this.tableService
                    .get(name, true)
                    .orElseThrow(IllegalStateException::new);

            eventBus.postAsync(new MetacatUpdateTablePostEvent(name, metacatRequestContext, oldTable, currentTable));
            return null;
        });
    }
}
