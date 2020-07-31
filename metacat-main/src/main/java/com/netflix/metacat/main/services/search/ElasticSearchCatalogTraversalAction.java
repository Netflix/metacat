/*
 *  Copyright 2019 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */


package com.netflix.metacat.main.services.search;

import com.google.common.collect.Lists;
import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.DatabaseDto;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.server.connectors.exception.DatabaseNotFoundException;
import com.netflix.metacat.common.server.events.MetacatDeleteTablePostEvent;
import com.netflix.metacat.common.server.events.MetacatEventBus;
import com.netflix.metacat.common.server.monitoring.Metrics;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.common.server.usermetadata.TagService;
import com.netflix.metacat.common.server.usermetadata.UserMetadataService;
import com.netflix.metacat.main.services.CatalogTraversal;
import com.netflix.metacat.main.services.CatalogTraversalAction;
import com.netflix.metacat.main.services.DatabaseService;
import com.netflix.metacat.main.services.GetDatabaseServiceParameters;
import com.netflix.metacat.main.services.GetTableServiceParameters;
import com.netflix.metacat.main.services.TableService;
import com.netflix.spectator.api.Registry;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * This class does a refresh of all the metadata entities from original data sources to elastic search.
 *
 * @author amajumdar
 */
@Slf4j
public class ElasticSearchCatalogTraversalAction implements CatalogTraversalAction {
    private final Config config;
    private final DatabaseService databaseService;
    private final TableService tableService;
    private final ElasticSearchUtil elasticSearchUtil;
    private final UserMetadataService userMetadataService;
    private final TagService tagService;
    private final MetacatEventBus eventBus;
    private final Registry registry;

    /**
     * Constructor.
     *
     * @param config              System config
     * @param eventBus            Event bus
     * @param databaseService     Database service
     * @param tableService        Table service
     * @param userMetadataService User metadata service
     * @param tagService          Tag service
     * @param registry            registry of spectator
     * @param elasticSearchUtil   ElasticSearch client wrapper
     */
    public ElasticSearchCatalogTraversalAction(
        @Nonnull @NonNull final Config config,
        @Nonnull @NonNull final MetacatEventBus eventBus,
        @Nonnull @NonNull final DatabaseService databaseService,
        @Nonnull @NonNull final TableService tableService,
        @Nonnull @NonNull final UserMetadataService userMetadataService,
        @Nonnull @NonNull final TagService tagService,
        @Nonnull @NonNull final ElasticSearchUtil elasticSearchUtil,
        @Nonnull @NonNull final Registry registry
    ) {
        this.config = config;
        this.eventBus = eventBus;
        this.databaseService = databaseService;
        this.tableService = tableService;
        this.userMetadataService = userMetadataService;
        this.tagService = tagService;
        this.elasticSearchUtil = elasticSearchUtil;
        this.registry = registry;
    }

    @Override
    public void done(final CatalogTraversal.Context context) {
        deleteUnmarkedEntities(context);
    }

    private void deleteUnmarkedEntities(final CatalogTraversal.Context context) {
        log.info("Start: Delete unmarked entities");
        //
        // get unmarked qualified names
        // check if it not exists
        // delete
        //
        elasticSearchUtil.refresh();
        final MetacatRequestContext requestContext = MetacatRequestContext.builder().userName("admin").
            clientAppName("metacat-refresh")
            .apiUri("esRefresh")
            .scheme("internal").build();


        final List<DatabaseDto> unmarkedDatabaseDtos = elasticSearchUtil
            .getQualifiedNamesByMarkerByNames("database", context.getQNames(), context.getStartInstant(),
                context.getExcludeQNames(),
                DatabaseDto.class);
        if (!unmarkedDatabaseDtos.isEmpty()) {
            if (unmarkedDatabaseDtos.size() <= config.getElasticSearchThresholdUnmarkedDatabasesDelete()) {
                log.info("Traversal Done: Start: Delete unmarked databases({})", unmarkedDatabaseDtos.size());
                final List<String> unmarkedDatabaseNames = Lists.newArrayList();
                final List<DatabaseDto> deleteDatabaseDtos = unmarkedDatabaseDtos.stream().filter(databaseDto -> {
                    boolean result = false;
                    try {
                        unmarkedDatabaseNames.add(databaseDto.getName().toString());
                        final DatabaseDto dto = databaseService.get(databaseDto.getName(),
                            GetDatabaseServiceParameters.builder()
                                .includeUserMetadata(false)
                                .includeTableNames(false)
                                .disableOnReadMetadataIntercetor(false)
                                .build());
                        if (dto == null) {
                            result = true;
                        }
                    } catch (DatabaseNotFoundException de) {
                        result = true;
                    } catch (Exception e) {
                        log.warn("Ignoring exception during deleteUnmarkedEntities for {}. Message: {}",
                            databaseDto.getName(), e.getMessage());
                    }
                    return result;
                }).collect(Collectors.toList());
                log.info("Unmarked databases({}): {}", unmarkedDatabaseNames.size(), unmarkedDatabaseNames);
                log.info("Deleting databases({})", deleteDatabaseDtos.size());
                if (!deleteDatabaseDtos.isEmpty()) {
                    final List<QualifiedName> deleteDatabaseQualifiedNames = deleteDatabaseDtos.stream()
                        .map(DatabaseDto::getName)
                        .collect(Collectors.toList());
                    final List<String> deleteDatabaseNames = deleteDatabaseQualifiedNames.stream().map(
                        QualifiedName::toString).collect(Collectors.toList());
                    log.info("Deleting databases({}): {}", deleteDatabaseNames.size(), deleteDatabaseNames);
                    userMetadataService.deleteDefinitionMetadata(deleteDatabaseQualifiedNames);
                    elasticSearchUtil.softDelete("database", deleteDatabaseNames, requestContext);
                }
                log.info("End: Delete unmarked databases({})", unmarkedDatabaseDtos.size());
            } else {
                log.info("Count of unmarked databases({}) is more than the threshold {}", unmarkedDatabaseDtos.size(),
                    config.getElasticSearchThresholdUnmarkedDatabasesDelete());
                registry.counter(
                    registry.createId(Metrics.CounterElasticSearchUnmarkedDatabaseThreshholdReached.getMetricName()))
                    .increment();
            }
        }

        final List<TableDto> unmarkedTableDtos = elasticSearchUtil
            .getQualifiedNamesByMarkerByNames("table",
                context.getQNames(), context.getStartInstant(), context.getExcludeQNames(), TableDto.class);
        if (!unmarkedTableDtos.isEmpty()) {
            if (unmarkedTableDtos.size() <= config.getElasticSearchThresholdUnmarkedTablesDelete()) {
                log.info("Start: Delete unmarked tables({})", unmarkedTableDtos.size());
                final List<String> unmarkedTableNames = Lists.newArrayList();
                final List<TableDto> deleteTableDtos = unmarkedTableDtos.stream().filter(tableDto -> {
                    boolean result = false;
                    try {
                        unmarkedTableNames.add(tableDto.getName().toString());
                        final Optional<TableDto> dto = tableService.get(tableDto.getName(),
                            GetTableServiceParameters.builder()
                                .includeDataMetadata(false)
                                .disableOnReadMetadataIntercetor(false)
                                .includeInfo(true)
                                .includeDefinitionMetadata(false)
                                .build());
                        if (!dto.isPresent()) {
                            result = true;
                        }
                    } catch (Exception e) {
                        log.warn("Ignoring exception during deleteUnmarkedEntities for {}. Message: {}",
                            tableDto.getName(), e.getMessage());
                    }
                    return result;
                }).collect(Collectors.toList());
                log.info("Unmarked tables({}): {}", unmarkedTableNames.size(), unmarkedTableNames);
                log.info("Deleting tables({})", deleteTableDtos.size());
                if (!deleteTableDtos.isEmpty()) {
                    final List<String> deleteTableNames = deleteTableDtos.stream().map(
                        dto -> dto.getName().toString()).collect(Collectors.toList());
                    log.info("Deleting tables({}): {}", deleteTableNames.size(), deleteTableNames);
                    userMetadataService.deleteMetadata("admin", Lists.newArrayList(deleteTableDtos));

                    // Publish event. Elasticsearch event handler will take care of updating the index already
                    // TODO: Re-evaluate events vs. direct calls for these types of situations like in Genie
                    deleteTableDtos.forEach(
                        tableDto -> {
                            tagService.delete(tableDto.getName(), false);
                            this.eventBus.post(
                                new MetacatDeleteTablePostEvent(tableDto.getName(), requestContext, this, tableDto)
                            );
                        }
                    );
                }
                log.info("Traversal Done: End: Delete unmarked tables({})", unmarkedTableDtos.size());
            } else {
                log.info("Count of unmarked tables({}) is more than the threshold {}", unmarkedTableDtos.size(),
                    config.getElasticSearchThresholdUnmarkedTablesDelete());
                registry.counter(
                    registry.createId(Metrics.CounterElasticSearchUnmarkedTableThreshholdReached.getMetricName()))
                    .increment();

            }
        }
        log.info("End: Delete unmarked entities");
    }

    /**
     * Save all databases to index it in elastic search.
     *
     * @param context     traversal context
     * @param dtos        database dtos
     */
    @Override
    public void applyDatabases(final CatalogTraversal.Context context, final List<DatabaseDto> dtos) {
        final List<ElasticSearchDoc> docs = dtos.stream()
            .filter(Objects::nonNull)
            .map(dto -> new ElasticSearchDoc(dto.getName().toString(), dto, "admin", false, context.getRunId()))
            .collect(Collectors.toList());
        elasticSearchUtil.save(ElasticSearchDoc.Type.database.name(), docs);
    }

    /**
     * Save all tables to index it in elastic search.
     *
     * @param context     traversal context
     * @param dtos         table dtos
     */
    @Override
    public void applyTables(final CatalogTraversal.Context context, final List<Optional<TableDto>> dtos) {
        final List<ElasticSearchDoc> docs = dtos.stream().filter(dto -> dto != null && dto.isPresent()).map(
            tableDtoOptional -> {
                final TableDto dto = tableDtoOptional.get();
                final String userName = dto.getAudit() != null ? dto.getAudit().getCreatedBy() : "admin";
                return new ElasticSearchDoc(dto.getName().toString(), dto, userName, false, context.getRunId());
            }).collect(Collectors.toList());
        elasticSearchUtil.save(ElasticSearchDoc.Type.table.name(), docs);
    }
}
