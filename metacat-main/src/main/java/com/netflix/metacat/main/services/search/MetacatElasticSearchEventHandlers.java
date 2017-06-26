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
import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.dto.DatabaseDto;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.json.MetacatJsonLocator;
import com.netflix.metacat.common.server.events.MetacatCreateDatabasePostEvent;
import com.netflix.metacat.common.server.events.MetacatCreateTablePostEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteDatabasePostEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteTablePartitionPostEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteTablePostEvent;
import com.netflix.metacat.common.server.events.MetacatRenameTablePostEvent;
import com.netflix.metacat.common.server.events.MetacatSaveTablePartitionPostEvent;
import com.netflix.metacat.common.server.events.MetacatUpdateTablePostEvent;
import com.netflix.metacat.common.server.monitoring.Metrics;
import com.netflix.spectator.api.Registry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Event handlers for elastic search indexing.
 */
@Slf4j
public class MetacatElasticSearchEventHandlers {
    private final ElasticSearchUtil es;
    private final Registry registry;
    private MetacatJsonLocator metacatJsonLocator;
    /**
     * Constructor.
     *
     * @param es elastic search util
     * @param registry registry to spectator
     */
    public MetacatElasticSearchEventHandlers(final ElasticSearchUtil es,
                                             final Registry registry) {
        this.es = es;
        this.registry = registry;
        this.metacatJsonLocator = new MetacatJsonLocator();
    }

    /**
     * Subscriber.
     *
     * @param event event
     */
    @EventListener
    public void metacatCreateDatabasePostEventHandler(final MetacatCreateDatabasePostEvent event) {
        log.debug("Received CreateDatabaseEvent {}", event);
        registry.counter(Metrics.CounterElasticSearchDatabaseCreate.name()).increment();
        final DatabaseDto dto = event.getDatabase();
        final ElasticSearchDoc doc = new ElasticSearchDoc(dto.getName().toString(), dto,
            event.getRequestContext().getUserName(), false);
        es.save(ElasticSearchDoc.Type.database.name(), doc.getId(), es.toJsonString(doc));
    }

    /**
     * Subscriber.
     *
     * @param event event
     */
    @EventListener
    public void metacatCreateTablePostEventHandler(final MetacatCreateTablePostEvent event) {
        log.debug("Received CreateTableEvent {}", event);
        registry.counter(Metrics.CounterElasticSearchTableCreate.name()).increment();
        final TableDto dto = event.getTable();
        final ElasticSearchDoc doc = new ElasticSearchDoc(dto.getName().toString(), dto,
            event.getRequestContext().getUserName(), false);
        es.save(ElasticSearchDoc.Type.table.name(), doc.getId(), es.toJsonString(doc));
    }

    /**
     * Subscriber.
     *
     * @param event event
     */
    @EventListener
    public void metacatDeleteDatabasePostEventHandler(final MetacatDeleteDatabasePostEvent event) {
        log.debug("Received DeleteDatabaseEvent {}", event);
        registry.counter(Metrics.CounterElasticSearchDatabaseDelete.name()).increment();
        final DatabaseDto dto = event.getDatabase();
        es.softDelete(ElasticSearchDoc.Type.database.name(), dto.getName().toString(), event.getRequestContext());
    }

    /**
     * Subscriber.
     *
     * @param event event
     */
    @EventListener
    public void metacatDeleteTablePostEventHandler(final MetacatDeleteTablePostEvent event) {
        log.debug("Received DeleteTableEvent {}", event);
        registry.counter(Metrics.CounterElasticSearchTableDelete.name()).increment();
        final TableDto dto = event.getTable();
        es.softDelete(ElasticSearchDoc.Type.table.name(), dto.getName().toString(), event.getRequestContext());
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
     *
     * @param event event
     */
    @EventListener
    public void metacatDeleteTablePartitionPostEventHandler(final MetacatDeleteTablePartitionPostEvent event) {
        log.debug("Received DeleteTablePartitionEvent {}", event);
        registry.counter(Metrics.CounterElasticSearchPartitionDelete.name()).increment();
        final List<String> partitionIds = event.getPartitionIds();
        final List<String> esPartitionIds = partitionIds.stream()
            .map(partitionId -> event.getName().toString() + "/" + partitionId).collect(Collectors.toList());
        es.softDelete(ElasticSearchDoc.Type.partition.name(), esPartitionIds, event.getRequestContext());
    }

    /**
     * Subscriber.
     *
     * @param event event
     */
    @EventListener
    public void metacatRenameTablePostEventHandler(final MetacatRenameTablePostEvent event) {
        log.debug("Received RenameTableEvent {}", event);
        registry.counter(Metrics.CounterElasticSearchTableRename.name()).increment();
        es.delete(ElasticSearchDoc.Type.table.name(), event.getName().toString());

        final TableDto dto = event.getCurrentTable();
        final ElasticSearchDoc doc = new ElasticSearchDoc(dto.getName().toString(), dto,
            event.getRequestContext().getUserName(), false);
        es.save(ElasticSearchDoc.Type.table.name(), doc.getId(), es.toJsonString(doc));
    }

    /**
     * Subscriber.
     *
     * @param event event
     */
    @EventListener
    public void metacatUpdateTablePostEventHandler(final MetacatUpdateTablePostEvent event) {
        log.debug("Received UpdateTableEvent {}", event);
        registry.counter(Metrics.CounterElasticSearchTableUpdate.name()).increment();
        final TableDto dto = event.getCurrentTable();

        final ElasticSearchDoc doc = new ElasticSearchDoc(dto.getName().toString(), dto,
            event.getRequestContext().getUserName(), false);
        final ElasticSearchDoc oldDoc = es.get(ElasticSearchDoc.Type.table.name(), doc.getId());
        es.save(ElasticSearchDoc.Type.table.name(), doc.getId(), es.toJsonString(doc));
        if (oldDoc == null || oldDoc.getDto() == null
            || !Objects.equals(((TableDto) oldDoc.getDto()).getDataMetadata(), dto.getDataMetadata())) {
            updateEntitiesWithSameUri(ElasticSearchDoc.Type.table.name(), dto, event.getRequestContext());
        }
    }

    private void updateEntitiesWithSameUri(final String metadataType, final TableDto dto,
                                           final MetacatRequestContext metacatRequestContext) {
        if (dto.isDataExternal()) {
            final List<String> ids = es.getTableIdsByUri(metadataType, dto.getDataUri());
            ids.remove(dto.getName().toString());
            if (!ids.isEmpty()) {
                final ObjectNode node = metacatJsonLocator.emptyObjectNode();
                node.set("dataMetadata", dto.getDataMetadata());
                es.updates(ElasticSearchDoc.Type.table.name(), ids, metacatRequestContext, node);
            }
        }
    }

    /**
     * Subscriber.
     *
     * @param event event
     */
    @EventListener
    public void metacatSaveTablePartitionPostEventHandler(final MetacatSaveTablePartitionPostEvent event) {
        log.debug("Received SaveTablePartitionEvent {}", event);
        registry.counter(Metrics.CounterElasticSearchPartitionSave.name()).increment();
        final List<PartitionDto> partitionDtos = event.getPartitions();
        final MetacatRequestContext context = event.getRequestContext();
        final List<ElasticSearchDoc> docs = partitionDtos.stream()
            .map(dto -> new ElasticSearchDoc(dto.getName().toString(), dto, context.getUserName(), false))
            .collect(Collectors.toList());
        es.save(ElasticSearchDoc.Type.partition.name(), docs);
    }
}
