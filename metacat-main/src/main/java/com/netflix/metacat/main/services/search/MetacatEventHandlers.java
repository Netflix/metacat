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

package com.netflix.metacat.main.services.search;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.eventbus.Subscribe;
import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.dto.DatabaseDto;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.json.MetacatJsonLocator;
import com.netflix.metacat.common.server.events.MetacatCreateDatabasePostEvent;
import com.netflix.metacat.common.server.events.MetacatCreateMViewPostEvent;
import com.netflix.metacat.common.server.events.MetacatCreateTablePostEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteDatabasePostEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteMViewPartitionPostEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteMViewPostEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteTablePartitionPostEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteTablePostEvent;
import com.netflix.metacat.common.server.events.MetacatRenameTablePostEvent;
import com.netflix.metacat.common.server.events.MetacatSaveMViewPartitionPostEvent;
import com.netflix.metacat.common.server.events.MetacatSaveTablePartitionPostEvent;
import com.netflix.metacat.common.server.events.MetacatUpdateMViewPostEvent;
import com.netflix.metacat.common.server.events.MetacatUpdateTablePostEvent;
import com.netflix.metacat.main.services.impl.MViewServiceImpl;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Event handlers for elastic search indexing.
 */
@Slf4j
public class MetacatEventHandlers {
    private final ElasticSearchUtil es;

    /**
     * Constructor.
     * @param es elastic search util
     */
    @Inject
    public MetacatEventHandlers(final ElasticSearchUtil es) {
        this.es = es;
    }

    /**
     * Subscriber.
     * @param event event
     */
    @Subscribe
    public void metacatCreateDatabasePostEventHandler(final MetacatCreateDatabasePostEvent event) {
        final DatabaseDto dto = event.getDatabase();
        final ElasticSearchDoc doc = new ElasticSearchDoc(dto.getName().toString(), dto,
            event.getRequestContext().getUserName(), false);
        es.save(ElasticSearchDoc.Type.database.name(), doc.getId(), doc.toJsonString());
    }

    /**
     * Subscriber.
     * @param event event
     */
    @Subscribe
    public void metacatCreateMViewPostEventHandler(final MetacatCreateMViewPostEvent event) {
        final TableDto dto = event.getTable();
        final ElasticSearchDoc doc = new ElasticSearchDoc(dto.getName().toString(), dto,
            event.getRequestContext().getUserName(), false);
        es.save(ElasticSearchDoc.Type.mview.name(), doc.getId(), doc.toJsonString());
    }

    /**
     * Subscriber.
     * @param event event
     */
    @Subscribe
    public void metacatCreateTablePostEventHandler(final MetacatCreateTablePostEvent event) {
        final TableDto dto = event.getTable();
        final ElasticSearchDoc doc = new ElasticSearchDoc(dto.getName().toString(), dto,
            event.getRequestContext().getUserName(), false);
        es.save(ElasticSearchDoc.Type.table.name(), doc.getId(), doc.toJsonString());
    }

    /**
     * Subscriber.
     * @param event event
     */
    @Subscribe
    public void metacatDeleteDatabasePostEventHandler(final MetacatDeleteDatabasePostEvent event) {
        final DatabaseDto dto = event.getDatabase();
        es.softDelete(ElasticSearchDoc.Type.database.name(), dto.getName().toString(), event.getRequestContext());
    }

    /**
     * Subscriber.
     * @param event event
     */
    @Subscribe
    public void metacatDeleteMViewPostEventHandler(final MetacatDeleteMViewPostEvent event) {
        final TableDto dto = event.getTable();
        es.softDelete(ElasticSearchDoc.Type.mview.name(), dto.getName().toString(), event.getRequestContext());
    }

    /**
     * Subscriber.
     * @param event event
     */
    @Subscribe
    public void metacatDeleteTablePostEventHandler(final MetacatDeleteTablePostEvent event) {
        final TableDto dto = event.getTable();
        if (MViewServiceImpl.VIEW_DB_NAME.equals(dto.getName().getDatabaseName())) {
            es.softDelete(ElasticSearchDoc.Type.mview.name(), dto.getName().toString(), event.getRequestContext());
        } else {
            es.softDelete(ElasticSearchDoc.Type.table.name(), dto.getName().toString(), event.getRequestContext());
        }
        try {
            final List<String> partitionIdsToBeDeleted =
                es.getIdsByQualifiedName(ElasticSearchDoc.Type.partition.name(), dto.getName());
            es.delete(ElasticSearchDoc.Type.partition.name(), partitionIdsToBeDeleted);
        } catch (Exception e) {
            log.warn("Failed deleting the partitions for the dropped table/view:{}", dto.getName().toString());
        }
    }

    /**
     * Subscriber.
     * @param event event
     */
    @Subscribe
    public void metacatDeleteMViewPartitionPostEventHandler(final MetacatDeleteMViewPartitionPostEvent event) {
        final List<String> partitionIds = event.getPartitionIds();
        final List<String> esPartitionIds = partitionIds.stream()
            .map(partitionId -> event.getName().toString() + "/" + partitionId).collect(Collectors.toList());
        es.softDelete(ElasticSearchDoc.Type.partition.name(), esPartitionIds, event.getRequestContext());
    }

    /**
     * Subscriber.
     * @param event event
     */
    @Subscribe
    public void metacatDeleteTablePartitionPostEventHandler(final MetacatDeleteTablePartitionPostEvent event) {
        final List<String> partitionIds = event.getPartitionIds();
        final List<String> esPartitionIds = partitionIds.stream()
            .map(partitionId -> event.getName().toString() + "/" + partitionId).collect(Collectors.toList());
        es.softDelete(ElasticSearchDoc.Type.partition.name(), esPartitionIds, event.getRequestContext());
    }

    /**
     * Subscriber.
     * @param event event
     */
    @Subscribe
    public void metacatRenameTablePostEventHandler(final MetacatRenameTablePostEvent event) {
        es.delete(ElasticSearchDoc.Type.table.name(), event.getName().toString());

        final TableDto dto = event.getCurrentTable();
        final ElasticSearchDoc doc = new ElasticSearchDoc(dto.getName().toString(), dto,
            event.getRequestContext().getUserName(), false);
        es.save(ElasticSearchDoc.Type.table.name(), doc.getId(), doc.toJsonString());
    }

    /**
     * Subscriber.
     * @param event event
     */
    @Subscribe
    public void metacatUpdateMViewPostEventHandler(final MetacatUpdateMViewPostEvent event) {
        final TableDto dto = event.getTable();
        final ElasticSearchDoc doc = new ElasticSearchDoc(dto.getName().toString(), dto,
            event.getRequestContext().getUserName(), false);
        es.save(ElasticSearchDoc.Type.mview.name(), doc.getId(), doc.toJsonString());
    }

    /**
     * Subscriber.
     * @param event event
     */
    @Subscribe
    public void metacatUpdateTablePostEventHandler(final MetacatUpdateTablePostEvent event) {
        final TableDto dto = event.getCurrentTable();
        final ElasticSearchDoc doc = new ElasticSearchDoc(dto.getName().toString(), dto,
            event.getRequestContext().getUserName(), false);
        es.save(ElasticSearchDoc.Type.table.name(), doc.getId(), doc.toJsonString());
        updateEntitiesWIthSameUri(ElasticSearchDoc.Type.table.name(), dto, event.getRequestContext());
    }

    private void updateEntitiesWIthSameUri(final String metadataType, final TableDto dto,
        final MetacatRequestContext metacatRequestContext) {
        if (dto.isDataExternal()) {
            final List<String> ids = es.getTableIdsByUri(metadataType, dto.getDataUri());
            final ObjectNode node = MetacatJsonLocator.INSTANCE.emptyObjectNode();
            node.put("dataMetadata", dto.getDataMetadata());
            es.updates(ElasticSearchDoc.Type.table.name(), ids, metacatRequestContext, node);
        }
    }

    /**
     * Subscriber.
     * @param event event
     */
    @Subscribe
    public void metacatSaveMViewPartitionPostEventHandler(final MetacatSaveMViewPartitionPostEvent event) {
        final List<PartitionDto> partitionDtos = event.getPartitions();
        final MetacatRequestContext context = event.getRequestContext();
        final List<ElasticSearchDoc> docs = partitionDtos.stream()
            .map(dto -> new ElasticSearchDoc(dto.getName().toString(), dto, context.getUserName(), false))
            .collect(Collectors.toList());
        es.save(ElasticSearchDoc.Type.partition.name(), docs);
    }

    /**
     * Subscriber.
     * @param event event
     */
    @Subscribe
    public void metacatSaveTablePartitionPostEventHandler(final MetacatSaveTablePartitionPostEvent event) {
        final List<PartitionDto> partitionDtos = event.getPartitions();
        final MetacatRequestContext context = event.getRequestContext();
        final List<ElasticSearchDoc> docs = partitionDtos.stream()
            .map(dto -> new ElasticSearchDoc(dto.getName().toString(), dto, context.getUserName(), false))
            .collect(Collectors.toList());
        es.save(ElasticSearchDoc.Type.partition.name(), docs);
    }
}
