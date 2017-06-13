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
import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.Subscribe;
import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.dto.DatabaseDto;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.json.MetacatJsonLocator;
import com.netflix.metacat.common.server.Config;
import com.netflix.metacat.common.server.events.MetacatCreateDatabasePostEvent;
import com.netflix.metacat.common.server.events.MetacatCreateTablePostEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteDatabasePostEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteTablePartitionPostEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteTablePostEvent;
import com.netflix.metacat.common.server.events.MetacatRenameTablePostEvent;
import com.netflix.metacat.common.server.events.MetacatSaveTablePartitionPostEvent;
import com.netflix.metacat.common.server.events.MetacatUpdateTablePostEvent;
import com.netflix.metacat.common.server.monitoring.TimerWrapper;
import com.netflix.servo.monitor.DynamicTimer;
import com.netflix.servo.monitor.MonitorConfig;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Event handlers for elastic search indexing.
 */
@Slf4j
public class MetacatEventHandlers {
    private final ElasticSearchUtil es;
    private final Config config;

    /**
     * Constructor.
     * @param es elastic search util
     * @param config configurations
     */
    @Inject
    public MetacatEventHandlers(final ElasticSearchUtil es, final Config config) {
        this.es = es;
        this.config = config;
    }

    /**
     * Subscriber.
     * @param event event
     */
    @Subscribe
    @AllowConcurrentEvents
    public void metacatCreateDatabasePostEventHandler(final MetacatCreateDatabasePostEvent event) {
        log.debug("Received CreateDatabaseEvent {}", event);
        DynamicTimer.record(MonitorConfig.builder("metacat.elasticsearch.events.delay")
                .withTag("metacat.event.type", "database.create").build(),
            System.currentTimeMillis() - event.getRequestContext().getTimestamp());
        final TimerWrapper timer = TimerWrapper.createStarted("metacat.timer.elasticsearch.events.database.create");
        try {
            final DatabaseDto dto = event.getDatabase();
            final ElasticSearchDoc doc = new ElasticSearchDoc(dto.getName().toString(), dto,
                event.getRequestContext().getUserName(), false);
            es.save(ElasticSearchDoc.Type.database.name(), doc.getId(), doc.toJsonString());
        } finally {
            log.info("*** ES Time taken to complete {} is {} ms", "database.create", timer.stop());
        }
    }

    /**
     * Subscriber.
     * @param event event
     */
    @Subscribe
    @AllowConcurrentEvents
    public void metacatCreateTablePostEventHandler(final MetacatCreateTablePostEvent event) {
        log.debug("Received CreateTableEvent {}", event);
        DynamicTimer.record(MonitorConfig.builder("metacat.elasticsearch.events.delay")
                .withTag("metacat.event.type", "table.create").build(),
            System.currentTimeMillis() - event.getRequestContext().getTimestamp());
        final TimerWrapper timer = TimerWrapper.createStarted("metacat.timer.elasticsearch.events.table.create");
        try {
            final TableDto dto = event.getTable();
            final ElasticSearchDoc doc = new ElasticSearchDoc(dto.getName().toString(), dto,
                event.getRequestContext().getUserName(), false);
            es.save(ElasticSearchDoc.Type.table.name(), doc.getId(), doc.toJsonString());
        } finally {
            log.info("*** ES Time taken to complete {} is {} ms", "table.create", timer.stop());
        }
    }

    /**
     * Subscriber.
     * @param event event
     */
    @Subscribe
    @AllowConcurrentEvents
    public void metacatDeleteDatabasePostEventHandler(final MetacatDeleteDatabasePostEvent event) {
        log.debug("Received DeleteDatabaseEvent {}", event);
        DynamicTimer.record(MonitorConfig.builder("metacat.elasticsearch.events.delay")
                .withTag("metacat.event.type", "database.delete").build(),
            System.currentTimeMillis() - event.getRequestContext().getTimestamp());
        final TimerWrapper timer = TimerWrapper.createStarted("metacat.timer.elasticsearch.events.database.delete");
        try {
            final DatabaseDto dto = event.getDatabase();
            es.softDelete(ElasticSearchDoc.Type.database.name(), dto.getName().toString(), event.getRequestContext());
        } finally {
            log.info("*** ES Time taken to complete {} is {} ms", "database.delete", timer.stop());
        }
    }

    /**
     * Subscriber.
     * @param event event
     */
    @Subscribe
    @AllowConcurrentEvents
    public void metacatDeleteTablePostEventHandler(final MetacatDeleteTablePostEvent event) {
        log.debug("Received DeleteTableEvent {}", event);
        DynamicTimer.record(MonitorConfig.builder("metacat.elasticsearch.events.delay")
                .withTag("metacat.event.type", "table.delete").build(),
            System.currentTimeMillis() - event.getRequestContext().getTimestamp());
        final TimerWrapper timer = TimerWrapper.createStarted("metacat.timer.elasticsearch.events.table.delete");
        try {
            final TableDto dto = event.getTable();
            es.softDelete(ElasticSearchDoc.Type.table.name(), dto.getName().toString(), event.getRequestContext());
            if (config.isElasticSearchPublishPartitionEnabled()) {
                try {
                    final List<String> partitionIdsToBeDeleted =
                        es.getIdsByQualifiedName(ElasticSearchDoc.Type.partition.name(), dto.getName());
                    es.delete(ElasticSearchDoc.Type.partition.name(), partitionIdsToBeDeleted);
                } catch (Exception e) {
                    log.warn("Failed deleting the partitions for the dropped table/view:{}", dto.getName().toString());
                }
            }
        } finally {
            log.info("*** ES Time taken to complete {} is {} ms", "table.delete", timer.stop());
        }
    }

    /**
     * Subscriber.
     * @param event event
     */
    @Subscribe
    @AllowConcurrentEvents
    public void metacatDeleteTablePartitionPostEventHandler(final MetacatDeleteTablePartitionPostEvent event) {
        if (config.isElasticSearchPublishPartitionEnabled()) {
            log.debug("Received DeleteTablePartitionEvent {}", event);
            DynamicTimer.record(MonitorConfig.builder("metacat.elasticsearch.events.delay")
                    .withTag("metacat.event.type", "partition.delete").build(),
                System.currentTimeMillis() - event.getRequestContext().getTimestamp());
            final TimerWrapper timer =
                TimerWrapper.createStarted("metacat.timer.elasticsearch.events.table.partition.delete");
            try {
                final List<String> partitionIds = event.getPartitionIds();
                final List<String> esPartitionIds = partitionIds.stream()
                    .map(partitionId -> event.getName().toString() + "/" + partitionId).collect(Collectors.toList());
                es.softDelete(ElasticSearchDoc.Type.partition.name(), esPartitionIds, event.getRequestContext());
            } finally {
                log.info("*** ES Time taken to complete {} is {} ms", "partition.delete", timer.stop());
            }
        }
    }

    /**
     * Subscriber.
     * @param event event
     */
    @Subscribe
    @AllowConcurrentEvents
    public void metacatRenameTablePostEventHandler(final MetacatRenameTablePostEvent event) {
        log.debug("Received RenameTableEvent {}", event);
        DynamicTimer.record(MonitorConfig.builder("metacat.elasticsearch.events.delay")
                .withTag("metacat.event.type", "table.rename").build(),
            System.currentTimeMillis() - event.getRequestContext().getTimestamp());
        final TimerWrapper timer =
            TimerWrapper.createStarted("metacat.timer.elasticsearch.events.table.rename");
        try {
            es.delete(ElasticSearchDoc.Type.table.name(), event.getName().toString());

            final TableDto dto = event.getCurrentTable();
            final ElasticSearchDoc doc = new ElasticSearchDoc(dto.getName().toString(), dto,
                event.getRequestContext().getUserName(), false);
            es.save(ElasticSearchDoc.Type.table.name(), doc.getId(), doc.toJsonString());
        } finally {
            log.info("*** ES Time taken to complete {} is {} ms", "table.rename", timer.stop());
        }
    }

    /**
     * Subscriber.
     * @param event event
     */
    @Subscribe
    @AllowConcurrentEvents
    public void metacatUpdateTablePostEventHandler(final MetacatUpdateTablePostEvent event) {
        log.debug("Received UpdateTableEvent {}", event);
        DynamicTimer.record(MonitorConfig.builder("metacat.elasticsearch.events.delay")
                .withTag("metacat.event.type", "table.update").build(),
            System.currentTimeMillis() - event.getRequestContext().getTimestamp());
        final TimerWrapper timer =
            TimerWrapper.createStarted("metacat.timer.elasticsearch.events.table.update");
        try {
            final TableDto dto = event.getCurrentTable();

            final ElasticSearchDoc doc = new ElasticSearchDoc(dto.getName().toString(), dto,
                event.getRequestContext().getUserName(), false);
            final ElasticSearchDoc oldDoc = es.get(ElasticSearchDoc.Type.table.name(), doc.getId());
            es.save(ElasticSearchDoc.Type.table.name(), doc.getId(), doc.toJsonString());
            if (oldDoc == null || oldDoc.getDto() == null
                || !Objects.equals(((TableDto) oldDoc.getDto()).getDataMetadata(), dto.getDataMetadata())) {
                updateEntitiesWithSameUri(ElasticSearchDoc.Type.table.name(), dto, event.getRequestContext());
            }
        } finally {
            log.info("*** ES Time taken to complete {} is {} ms", "table.update", timer.stop());
        }
    }

    private void updateEntitiesWithSameUri(final String metadataType, final TableDto dto,
        final MetacatRequestContext metacatRequestContext) {
        if (dto.isDataExternal()) {
            final List<String> ids = es.getTableIdsByUri(metadataType, dto.getDataUri());
            ids.remove(dto.getName().toString());
            if (!ids.isEmpty()) {
                final ObjectNode node = MetacatJsonLocator.INSTANCE.emptyObjectNode();
                node.set("dataMetadata", dto.getDataMetadata());
                es.updates(ElasticSearchDoc.Type.table.name(), ids, metacatRequestContext, node);
            }
        }
    }

    /**
     * Subscriber.
     * @param event event
     */
    @Subscribe
    @AllowConcurrentEvents
    public void metacatSaveTablePartitionPostEventHandler(final MetacatSaveTablePartitionPostEvent event) {
        if (config.isElasticSearchPublishPartitionEnabled()) {
            log.debug("Received SaveTablePartitionEvent {}", event);
            DynamicTimer.record(MonitorConfig.builder("metacat.elasticsearch.events.delay")
                    .withTag("metacat.event.type", "table.partition.save").build(),
                System.currentTimeMillis() - event.getRequestContext().getTimestamp());
            final TimerWrapper timer =
                TimerWrapper.createStarted("metacat.timer.elasticsearch.events.table.partition.save");
            try {
                final List<PartitionDto> partitionDtos = event.getPartitions();
                final MetacatRequestContext context = event.getRequestContext();
                final List<ElasticSearchDoc> docs = partitionDtos.stream()
                    .map(dto -> new ElasticSearchDoc(dto.getName().toString(), dto, context.getUserName(), false))
                    .collect(Collectors.toList());
                es.save(ElasticSearchDoc.Type.partition.name(), docs);
            } finally {
                log.info("*** ES Time taken to complete {} is {} ms", "table.partition.save", timer.stop());
            }
        }
    }
}
