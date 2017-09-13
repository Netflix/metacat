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
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Event handlers for elastic search indexing.
 */
@Slf4j
public class ElasticSearchEventHandlers {
    private final ElasticSearchUtil es;
    private final MetacatJsonLocator metacatJsonLocator;
    private final Config config;
    private final Timer databaseCreateEventsDelayTimer;
    private final Timer databaseCreateTimer;
    private final Timer tableCreateEventsDelayTimer;
    private final Timer tableCreateTimer;
    private final Timer databaseDeleteEventsDelayTimer;
    private final Timer databaseDeleteTimer;
    private final Timer tableDeleteEventsDelayTimer;
    private final Timer tableDeleteTimer;
    private final Timer partitionDeleteEventsDelayTimer;
    private final Timer partitionDeleteTimer;
    private final Timer tableRenameEventsDelayTimer;
    private final Timer tableRenameTimer;
    private final Timer tableUpdateEventsDelayTimer;
    private final Timer tableUpdateTimer;
    private final Timer partitionSaveEventsDelayTimer;
    private final Timer partitionSaveTimer;

    /**
     * Constructor.
     *
     * @param es       elastic search util
     * @param registry registry to spectator
     * @param config   configurations
     */
    public ElasticSearchEventHandlers(final ElasticSearchUtil es,
                                      final Registry registry,
                                      final Config config) {
        this.es = es;
        this.metacatJsonLocator = new MetacatJsonLocator();
        this.config = config;
        this.databaseCreateEventsDelayTimer = registry.timer(Metrics.TimerElasticSearchEventsDelay.getMetricName(),
            Metrics.TagEventsType.getMetricName(), "database.create");
        this.databaseCreateTimer = registry.timer(Metrics.TimerElasticSearchDatabaseCreate.getMetricName());
        this.tableCreateEventsDelayTimer = registry.timer(Metrics.TimerElasticSearchEventsDelay.getMetricName(),
            Metrics.TagEventsType.getMetricName(), "table.create");
        this.tableCreateTimer = registry.timer(Metrics.TimerElasticSearchTableCreate.getMetricName());
        this.databaseDeleteEventsDelayTimer = registry.timer(Metrics.TimerElasticSearchEventsDelay.getMetricName(),
            Metrics.TagEventsType.getMetricName(), "database.delete");
        this.databaseDeleteTimer = registry.timer(Metrics.TimerElasticSearchDatabaseDelete.getMetricName());
        this.tableDeleteEventsDelayTimer = registry.timer(Metrics.TimerElasticSearchEventsDelay.getMetricName(),
            Metrics.TagEventsType.getMetricName(), "table.delete");
        this.tableDeleteTimer = registry.timer(Metrics.TimerElasticSearchTableDelete.getMetricName());
        this.partitionDeleteEventsDelayTimer = registry.timer(Metrics.TimerElasticSearchEventsDelay.getMetricName(),
            Metrics.TagEventsType.getMetricName(), "partition.delete");
        this.partitionDeleteTimer = registry.timer(Metrics.TimerElasticSearchPartitionDelete.getMetricName());
        this.tableRenameEventsDelayTimer = registry.timer(Metrics.TimerElasticSearchEventsDelay.getMetricName(),
            Metrics.TagEventsType.getMetricName(), "table.rename");
        this.tableRenameTimer = registry.timer(Metrics.TimerElasticSearchTableRename.getMetricName());
        this.tableUpdateEventsDelayTimer = registry.timer(Metrics.TimerElasticSearchEventsDelay.getMetricName(),
            Metrics.TagEventsType.getMetricName(), "table.update");
        this.tableUpdateTimer = registry.timer(Metrics.TimerElasticSearchTableUpdate.getMetricName());
        this.partitionSaveEventsDelayTimer = registry.timer(Metrics.TimerElasticSearchEventsDelay.getMetricName(),
            Metrics.TagEventsType.getMetricName(), "partition.save");
        this.partitionSaveTimer = registry.timer(Metrics.TimerElasticSearchPartitionSave.getMetricName());
    }

    /**
     * Subscriber.
     *
     * @param event event
     */
    @EventListener
    public void metacatCreateDatabasePostEventHandler(final MetacatCreateDatabasePostEvent event) {
        log.debug("Received CreateDatabaseEvent {}", event);
        this.databaseCreateEventsDelayTimer
            .record(System.currentTimeMillis() - event.getRequestContext().getTimestamp(), TimeUnit.MILLISECONDS);
        this.databaseCreateTimer.record(() -> {
            final DatabaseDto dto = event.getDatabase();
            final ElasticSearchDoc doc = new ElasticSearchDoc(dto.getName().toString(), dto,
                event.getRequestContext().getUserName(), false);
            es.save(ElasticSearchDoc.Type.database.name(), doc.getId(), doc);
        });
    }

    /**
     * Subscriber.
     *
     * @param event event
     */
    @EventListener
    public void metacatCreateTablePostEventHandler(final MetacatCreateTablePostEvent event) {
        log.debug("Received CreateTableEvent {}", event);
        this.tableCreateEventsDelayTimer
            .record(System.currentTimeMillis() - event.getRequestContext().getTimestamp(), TimeUnit.MILLISECONDS);
        this.tableCreateTimer.record(() -> {
            final TableDto dto = event.getTable();
            final ElasticSearchDoc doc = new ElasticSearchDoc(dto.getName().toString(), dto,
                event.getRequestContext().getUserName(), false);
            es.save(ElasticSearchDoc.Type.table.name(), doc.getId(), doc);
        });
    }

    /**
     * Subscriber.
     *
     * @param event event
     */
    @EventListener
    public void metacatDeleteDatabasePostEventHandler(final MetacatDeleteDatabasePostEvent event) {
        log.debug("Received DeleteDatabaseEvent {}", event);
        this.databaseDeleteEventsDelayTimer
            .record(System.currentTimeMillis() - event.getRequestContext().getTimestamp(), TimeUnit.MILLISECONDS);
        this.databaseDeleteTimer.record(() -> {
            final DatabaseDto dto = event.getDatabase();
            es.softDelete(ElasticSearchDoc.Type.database.name(), dto.getName().toString(), event.getRequestContext());
        });
    }

    /**
     * Subscriber.
     *
     * @param event event
     */
    @EventListener
    public void metacatDeleteTablePostEventHandler(final MetacatDeleteTablePostEvent event) {
        log.debug("Received DeleteTableEvent {}", event);
        this.tableDeleteEventsDelayTimer
            .record(System.currentTimeMillis() - event.getRequestContext().getTimestamp(), TimeUnit.MILLISECONDS);
        this.tableDeleteTimer.record(() -> {
            final TableDto dto = event.getTable();
            es.softDelete(ElasticSearchDoc.Type.table.name(), dto.getName().toString(), event.getRequestContext());
            if (config.isElasticSearchPublishPartitionEnabled()) {
                try {
                    final List<String> partitionIdsToBeDeleted =
                        es.getIdsByQualifiedName(ElasticSearchDoc.Type.partition.name(), dto.getName());
                    es.delete(ElasticSearchDoc.Type.partition.name(), partitionIdsToBeDeleted);
                } catch (Exception e) {
                    log.warn("Failed deleting the partitions for the dropped table/view:{}", dto.getName());
                }
            }
        });
    }

    /**
     * Subscriber.
     *
     * @param event event
     */
    @EventListener
    public void metacatDeleteTablePartitionPostEventHandler(final MetacatDeleteTablePartitionPostEvent event) {
        log.debug("Received DeleteTablePartitionEvent {}", event);
        this.partitionDeleteEventsDelayTimer
            .record(System.currentTimeMillis() - event.getRequestContext().getTimestamp(), TimeUnit.MILLISECONDS);
        if (config.isElasticSearchPublishPartitionEnabled()) {
            this.partitionDeleteTimer.record(() -> {
                final List<String> partitionIds = event.getPartitionIds();
                final List<String> esPartitionIds = partitionIds.stream()
                    .map(partitionId -> event.getName().toString() + "/" + partitionId).collect(Collectors.toList());
                es.softDelete(ElasticSearchDoc.Type.partition.name(), esPartitionIds, event.getRequestContext());
            });
        }
    }

    /**
     * Subscriber.
     *
     * @param event event
     */
    @EventListener
    public void metacatRenameTablePostEventHandler(final MetacatRenameTablePostEvent event) {
        log.debug("Received RenameTableEvent {}", event);
        this.tableRenameEventsDelayTimer
            .record(System.currentTimeMillis() - event.getRequestContext().getTimestamp(), TimeUnit.MILLISECONDS);
        this.tableRenameTimer.record(() -> {
            es.delete(ElasticSearchDoc.Type.table.name(), event.getName().toString());

            final TableDto dto = event.getCurrentTable();
            final ElasticSearchDoc doc = new ElasticSearchDoc(dto.getName().toString(), dto,
                event.getRequestContext().getUserName(), false);
            es.save(ElasticSearchDoc.Type.table.name(), doc.getId(), doc);
        });
    }

    /**
     * Subscriber.
     *
     * @param event event
     */
    @EventListener
    public void metacatUpdateTablePostEventHandler(final MetacatUpdateTablePostEvent event) {
        log.debug("Received UpdateTableEvent {}", event);
        this.tableUpdateEventsDelayTimer
            .record(System.currentTimeMillis() - event.getRequestContext().getTimestamp(), TimeUnit.MILLISECONDS);
        this.tableUpdateTimer.record(() -> {
            final TableDto dto = event.getCurrentTable();

            final ElasticSearchDoc doc = new ElasticSearchDoc(dto.getName().toString(), dto,
                event.getRequestContext().getUserName(), false);
            final ElasticSearchDoc oldDoc = es.get(ElasticSearchDoc.Type.table.name(), doc.getId());
            es.save(ElasticSearchDoc.Type.table.name(), doc.getId(), doc);
            if (oldDoc == null || oldDoc.getDto() == null
                || !Objects.equals(((TableDto) oldDoc.getDto()).getDataMetadata(), dto.getDataMetadata())) {
                updateEntitiesWithSameUri(ElasticSearchDoc.Type.table.name(),
                    dto, event.getRequestContext().getUserName());
            }
        });
    }

    private void updateEntitiesWithSameUri(final String metadataType, final TableDto dto,
                                           final String userName) {
        if (dto.isDataExternal()) {
            final List<String> ids = es.getTableIdsByUri(metadataType, dto.getDataUri())
                .stream().filter(s -> !s.equals(dto.getName().toString())).collect(Collectors.toList());
            if (!ids.isEmpty()) {
                final ObjectNode node = metacatJsonLocator.emptyObjectNode();
                node.set(ElasticSearchDoc.Field.DATA_METADATA, dto.getDataMetadata());
                node.put(ElasticSearchDoc.Field.USER, userName);
                node.put(ElasticSearchDoc.Field.TIMESTAMP, java.time.Instant.now().toEpochMilli());
                es.updates(ElasticSearchDoc.Type.table.name(), ids, node);
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
        this.partitionSaveEventsDelayTimer
            .record(System.currentTimeMillis() - event.getRequestContext().getTimestamp(), TimeUnit.MILLISECONDS);
        if (config.isElasticSearchPublishPartitionEnabled()) {
            this.partitionSaveTimer.record(() -> {
                final List<PartitionDto> partitionDtos = event.getPartitions();
                final MetacatRequestContext context = event.getRequestContext();
                final List<ElasticSearchDoc> docs = partitionDtos.stream()
                    .map(dto -> new ElasticSearchDoc(dto.getName().toString(), dto, context.getUserName(), false))
                    .collect(Collectors.toList());
                es.save(ElasticSearchDoc.Type.partition.name(), docs);
            });
        }
    }
}
