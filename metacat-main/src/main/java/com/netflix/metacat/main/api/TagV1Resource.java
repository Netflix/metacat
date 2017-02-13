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

import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.api.TagV1;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.server.events.MetacatEventBus;
import com.netflix.metacat.common.server.events.MetacatUpdateTablePostEvent;
import com.netflix.metacat.common.server.exception.TableNotFoundException;
import com.netflix.metacat.common.usermetadata.TagService;
import com.netflix.metacat.common.util.MetacatContextManager;
import com.netflix.metacat.main.services.TableService;

import javax.inject.Inject;
import java.util.List;
import java.util.Set;

/**
 * Tag API implementation.
 * @author amajumdar
 */
public class TagV1Resource implements TagV1 {
    private TagService tagService;
    private MetacatEventBus eventBus;
    private TableService tableService;

    /**
     * Constructor.
     * @param eventBus event bus
     * @param tagService tag service
     * @param tableService table service
     */
    @Inject
    public TagV1Resource(final MetacatEventBus eventBus, final TagService tagService,
        final TableService tableService) {
        this.tagService = tagService;
        this.eventBus = eventBus;
        this.tableService = tableService;
    }

    @Override
    public Set<String> getTags() {
        return RequestWrapper.requestWrapper("TagV1Resource.getTags", tagService::getTags);
    }

    @Override
    public List<QualifiedName> list(
        final Set<String> includeTags,
        final Set<String> excludeTags,
        final String sourceName,
        final String databaseName,
        final String tableName) {
        return RequestWrapper.requestWrapper("TagV1Resource.list",
            () -> tagService.list(includeTags, excludeTags, sourceName, databaseName, tableName));
    }

    @Override
    public List<QualifiedName> search(
        final String tag,
        final String sourceName,
        final String databaseName,
        final String tableName) {
        return RequestWrapper.requestWrapper("TagV1Resource.search",
            () -> tagService.search(tag, sourceName, databaseName, tableName));
    }

    @Override
    public Set<String> setTableTags(
        final String catalogName,
        final String databaseName,
        final String tableName,
        final Set<String> tags) {
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        final QualifiedName name =
            RequestWrapper.qualifyName(() -> QualifiedName.ofTable(catalogName, databaseName, tableName));
        return RequestWrapper.requestWrapper(name, "TagV1Resource.setTableTags", () -> {
            if (!tableService.exists(name)) {
                throw new TableNotFoundException(name);
            }
            final TableDto oldTable = this.tableService
                .get(name, true)
                .orElseThrow(IllegalStateException::new);
            final Set<String> result = tagService.setTableTags(name, tags, true);
            final TableDto currentTable = this.tableService
                .get(name, true)
                .orElseThrow(IllegalStateException::new);
            eventBus.postAsync(new MetacatUpdateTablePostEvent(name, metacatRequestContext, oldTable, currentTable));
            return result;
        });
    }

    @Override
    public void removeTableTags(
        final String catalogName,
        final String databaseName,
        final String tableName,
        final Boolean deleteAll,
        final Set<String> tags) {
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        final QualifiedName name =
            RequestWrapper.qualifyName(() -> QualifiedName.ofTable(catalogName, databaseName, tableName));
        RequestWrapper.requestWrapper(name, "TagV1Resource.removeTableTags", () -> {
            if (!tableService.exists(name)) {
                // Delete tags if exists
                tagService.delete(name, false);
                throw new TableNotFoundException(name);
            }
            final TableDto oldTable = this.tableService
                .get(name, true)
                .orElseThrow(IllegalStateException::new);
            tagService.removeTableTags(name, deleteAll, tags, true);
            final TableDto currentTable = this.tableService
                .get(name, true)
                .orElseThrow(IllegalStateException::new);

            eventBus.postAsync(new MetacatUpdateTablePostEvent(name, metacatRequestContext, oldTable, currentTable));
            return null;
        });
    }
}
