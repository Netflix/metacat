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

import com.google.common.base.Functions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.CatalogDto;
import com.netflix.metacat.common.dto.CatalogMappingDto;
import com.netflix.metacat.common.dto.DatabaseDto;
import com.netflix.metacat.common.dto.GetPartitionsRequestDto;
import com.netflix.metacat.common.dto.HasMetadata;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.dto.SortOrder;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.server.connectors.exception.DatabaseNotFoundException;
import com.netflix.metacat.common.server.events.MetacatDeleteTablePostEvent;
import com.netflix.metacat.common.server.events.MetacatEventBus;
import com.netflix.metacat.common.server.monitoring.Metrics;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.common.server.usermetadata.TagService;
import com.netflix.metacat.common.server.usermetadata.UserMetadataService;
import com.netflix.metacat.common.server.util.MetacatContextManager;
import com.netflix.metacat.main.services.CatalogService;
import com.netflix.metacat.main.services.DatabaseService;
import com.netflix.metacat.main.services.GetDatabaseServiceParameters;
import com.netflix.metacat.main.services.GetTableServiceParameters;
import com.netflix.metacat.main.services.PartitionService;
import com.netflix.metacat.main.services.TableService;
import com.netflix.spectator.api.Registry;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.Instant;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * This class does a refresh of all the metadata entities from original data sources to elastic search.
 *
 * @author amajumdar
 */
@Slf4j
@Deprecated
public class ElasticSearchRefresh {
    private static final Predicate<Object> NOT_NULL = Objects::nonNull;
    private static AtomicBoolean isElasticSearchMetacatRefreshAlreadyRunning = new AtomicBoolean(false);

    private final CatalogService catalogService;
    private final Config config;
    private final DatabaseService databaseService;
    private final TableService tableService;
    private final PartitionService partitionService;
    private final ElasticSearchUtil elasticSearchUtil;
    private final UserMetadataService userMetadataService;
    private final TagService tagService;
    private final MetacatEventBus eventBus;

    private Instant refreshMarker;
    private String refreshMarkerText;

    private Registry registry;
    //  Fixed thread pool
    private ListeningExecutorService service;
    private ListeningExecutorService esService;
    private ExecutorService defaultService;

    /**
     * Constructor.
     *
     * @param config              System config
     * @param eventBus            Event bus
     * @param catalogService      Catalog service
     * @param databaseService     Database service
     * @param tableService        Table service
     * @param partitionService    Partition service
     * @param userMetadataService User metadata service
     * @param tagService          Tag service
     * @param registry            registry of spectator
     * @param elasticSearchUtil   ElasticSearch client wrapper
     */
    public ElasticSearchRefresh(
        @Nonnull @NonNull final Config config,
        @Nonnull @NonNull final MetacatEventBus eventBus,
        @Nonnull @NonNull final CatalogService catalogService,
        @Nonnull @NonNull final DatabaseService databaseService,
        @Nonnull @NonNull final TableService tableService,
        @Nonnull @NonNull final PartitionService partitionService,
        @Nonnull @NonNull final UserMetadataService userMetadataService,
        @Nonnull @NonNull final TagService tagService,
        @Nonnull @NonNull final ElasticSearchUtil elasticSearchUtil,
        @Nonnull @NonNull final Registry registry
    ) {
        this.config = config;
        this.eventBus = eventBus;
        this.catalogService = catalogService;
        this.databaseService = databaseService;
        this.tableService = tableService;
        this.partitionService = partitionService;
        this.userMetadataService = userMetadataService;
        this.tagService = tagService;
        this.elasticSearchUtil = elasticSearchUtil;
        this.registry = registry;
    }

    private static ExecutorService newFixedThreadPool(
        final int nThreads,
        final String threadFactoryName,
        final int queueSize
    ) {
        return new ThreadPoolExecutor(nThreads, nThreads,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(queueSize),
            new ThreadFactoryBuilder()
                .setNameFormat(threadFactoryName)
                .build(),
            (r, executor) -> {
                // this will block if the queue is full
                try {
                    executor.getQueue().put(r);
                } catch (InterruptedException e) {
                    throw Throwables.propagate(e);
                }
            });
    }

    /**
     * Does a sweep across all catalogs to refresh the same data in elastic search.
     */
    public void process() {
        final List<String> catalogNames = getCatalogNamesToRefresh();
        final List<QualifiedName> qNames = catalogNames.stream()
            .map(QualifiedName::ofCatalog).collect(Collectors.toList());
        _process(qNames, () -> _processCatalogs(catalogNames), "process", true, 1000);
    }

    /**
     * Does a sweep across given catalogs to refresh the same data in elastic search.
     *
     * @param catalogNames catalog anmes
     */
    public void processCatalogs(final List<String> catalogNames) {
        final List<QualifiedName> qNames = catalogNames.stream()
            .map(QualifiedName::ofCatalog).collect(Collectors.toList());
        _process(qNames, () -> _processCatalogs(catalogNames), "processCatalogs", true, 1000);
    }

    /**
     * Does a sweep across given catalog and databases to refresh the same data in elastic search.
     *
     * @param catalogName   catalog
     * @param databaseNames database names
     */
    public void processDatabases(final String catalogName, final List<String> databaseNames) {
        final List<QualifiedName> qNames = databaseNames.stream()
            .map(s -> QualifiedName.ofDatabase(catalogName, s)).collect(Collectors.toList());
        _process(qNames, () -> _processDatabases(QualifiedName.ofCatalog(catalogName), qNames), "processDatabases",
            true, 1000);
    }

    /**
     * Does a sweep across all catalogs to refresh the same data in elastic search.
     *
     * @param names qualified names
     */
    public void processPartitions(final List<QualifiedName> names) {
        List<QualifiedName> qNames = names;
        if (qNames == null || qNames.isEmpty()) {
            final List<String> catalogNames = Splitter.on(',').omitEmptyStrings().trimResults()
                .splitToList(config.getElasticSearchRefreshPartitionsIncludeCatalogs());
            qNames = catalogNames.stream()
                .map(QualifiedName::ofCatalog).collect(Collectors.toList());
        }
        final List<QualifiedName> qualifiedNames = qNames;
        _process(qualifiedNames, () -> _processPartitions(qualifiedNames), "processPartitions", false, 500);
    }

    @SuppressWarnings("checkstyle:methodname")
    private ListenableFuture<Void> _processPartitions(final List<QualifiedName> qNames) {
        final List<QualifiedName> excludeQualifiedNames = config.getElasticSearchRefreshExcludeQualifiedNames();
        final List<String> tables =
            elasticSearchUtil.getTableIdsByCatalogs(ElasticSearchDoc.Type.table.name(),
                qNames, excludeQualifiedNames);
        final List<ListenableFuture<ListenableFuture<Void>>> futures = tables.stream().map(s -> service.submit(() -> {
            final QualifiedName tableName = QualifiedName.fromString(s, false);
            final List<ListenableFuture<Void>> indexFutures = Lists.newArrayList();
            int offset = 0;
            int count;
            final Sort sort;
            if ("s3".equals(tableName.getCatalogName()) || "aegisthus".equals(tableName.getCatalogName())) {
                sort = new Sort("id", SortOrder.ASC);
            } else {
                sort = new Sort("part_id", SortOrder.ASC);
            }
            final Pageable pageable = new Pageable(10000, offset);
            do {
                final List<PartitionDto> partitionDtos =
                    partitionService.list(tableName, sort, pageable, true, true,
                        new GetPartitionsRequestDto(null, null, true, true));
                count = partitionDtos.size();
                if (!partitionDtos.isEmpty()) {
                    final List<List<PartitionDto>> partitionedPartitionDtos = Lists.partition(partitionDtos, 1000);
                    partitionedPartitionDtos.forEach(
                        subPartitionsDtos -> indexFutures.add(indexPartitionDtos(tableName, subPartitionsDtos)));
                    offset = offset + count;
                    pageable.setOffset(offset);
                }
            } while (count == 10000);
            return Futures.transform(Futures.successfulAsList(indexFutures),
                Functions.constant((Void) null), defaultService);
        })).collect(Collectors.toList());
        final ListenableFuture<Void> processPartitionsFuture = Futures.transformAsync(Futures.successfulAsList(futures),
            input -> {
                final List<ListenableFuture<Void>> inputFuturesWithoutNulls = input.stream().filter(NOT_NULL)
                    .collect(Collectors.toList());
                return Futures.transform(Futures.successfulAsList(inputFuturesWithoutNulls),
                    Functions.constant(null), defaultService);
            }, defaultService);
        return Futures.transformAsync(processPartitionsFuture, input -> {
            elasticSearchUtil.refresh();
            final List<ListenableFuture<Void>> cleanUpFutures = tables.stream()
                .map(s -> service
                    .submit(() -> partitionsCleanUp(QualifiedName.fromString(s, false), excludeQualifiedNames)))
                .collect(Collectors.toList());
            return Futures.transform(Futures.successfulAsList(cleanUpFutures),
                Functions.constant(null), defaultService);
        }, defaultService);
    }

    private Void partitionsCleanUp(final QualifiedName tableName, final List<QualifiedName> excludeQualifiedNames) {
            final List<PartitionDto> unmarkedPartitionDtos = elasticSearchUtil.getQualifiedNamesByMarkerByNames(
                ElasticSearchDoc.Type.partition.name(),
                Lists.newArrayList(tableName), refreshMarker, excludeQualifiedNames, PartitionDto.class);
            if (!unmarkedPartitionDtos.isEmpty()) {
                log.info("Start deleting unmarked partitions({}) for table {}",
                    unmarkedPartitionDtos.size(), tableName);
                try {
                    final List<String> unmarkedPartitionNames = unmarkedPartitionDtos.stream()
                        .map(p -> p.getDefinitionName().getPartitionName()).collect(Collectors.toList());
                    final Set<String> existingUnmarkedPartitionNames = Sets.newHashSet(
                        partitionService.getPartitionKeys(tableName, null, null,
                            new GetPartitionsRequestDto(null, unmarkedPartitionNames, false, true)));
                    final List<String> partitionIds = unmarkedPartitionDtos.stream()
                        .filter(p -> !existingUnmarkedPartitionNames.contains(
                            p.getDefinitionName().getPartitionName()))
                        .map(p -> p.getDefinitionName().toString()).collect(Collectors.toList());
                    if (!partitionIds.isEmpty()) {
                        log.info("Deleting unused partitions({}) for table {}:{}",
                            partitionIds.size(), tableName, partitionIds);
                        elasticSearchUtil.delete(ElasticSearchDoc.Type.partition.name(), partitionIds);
                        final List<HasMetadata> deletePartitionDtos = unmarkedPartitionDtos.stream()
                            .filter(
                                p -> !existingUnmarkedPartitionNames.contains(
                                    p.getDefinitionName().getPartitionName()))
                            .collect(Collectors.toList());
                        userMetadataService.deleteMetadata("admin", deletePartitionDtos);
                    }
                } catch (Exception e) {
                    log.warn("Failed deleting the unmarked partitions for table {}", tableName);
                }
                log.info("End deleting unmarked partitions for table {}", tableName);
            }
        return null;
    }

    @SuppressWarnings("checkstyle:methodname")
    private void _process(final List<QualifiedName> qNames, final Supplier<ListenableFuture<Void>> supplier,
                          final String requestName, final boolean delete, final int queueSize) {
        if (isElasticSearchMetacatRefreshAlreadyRunning.compareAndSet(false, true)) {
            final long start = registry.clock().wallTime();
            try {
                log.info("Start: Full refresh of metacat index in elastic search. Processing {} ...", qNames);
                final MetacatRequestContext context = MetacatRequestContext.builder()
                    .userName("admin")
                    .clientAppName("elasticSearchRefresher")
                    .apiUri("esRefresh")
                    .scheme("internal")
                    .build();
                MetacatContextManager.setContext(context);
                refreshMarker = Instant.now();
                refreshMarkerText = refreshMarker.toString();
                service = MoreExecutors
                    .listeningDecorator(newFixedThreadPool(10, "elasticsearch-refresher-%d", queueSize));
                esService = MoreExecutors
                    .listeningDecorator(newFixedThreadPool(5, "elasticsearch-refresher-es-%d", queueSize));
                defaultService = Executors.newSingleThreadExecutor();
                supplier.get().get(24, TimeUnit.HOURS);
                log.info("End: Full refresh of metacat index in elastic search");
                if (delete) {
                    deleteUnmarkedEntities(qNames, config.getElasticSearchRefreshExcludeQualifiedNames());
                }
            } catch (Exception e) {
                log.error("Full refresh of metacat index failed", e);
                registry.counter(registry.createId(Metrics.CounterElasticSearchRefresh.getMetricName())
                    .withTags(Metrics.tagStatusFailureMap)).increment();
            } finally {
                try {
                    shutdown(service);
                    shutdown(esService);
                    shutdown(defaultService);
                } finally {
                    isElasticSearchMetacatRefreshAlreadyRunning.set(false);
                    final long duration = registry.clock().wallTime() - start;
                    this.registry.timer(Metrics.TimerElasticSearchRefresh.getMetricName()
                        + "." + requestName).record(duration, TimeUnit.MILLISECONDS);
                    log.info("### Time taken to complete {} is {} ms", requestName, duration);
                }
            }

        } else {
            log.info("Full refresh of metacat index is already running.");
            registry.counter(registry.createId(Metrics.CounterElasticSearchRefreshAlreadyRunning.getMetricName()))
                .increment();
        }
    }

    private void shutdown(@Nullable final ExecutorService executorService) {
        if (executorService != null) {
            executorService.shutdown();
            try {
                // Wait a while for existing tasks to terminate
                if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                    executorService.shutdownNow(); // Cancel currently executing tasks
                    // Wait a while for tasks to respond to being cancelled
                    if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                        log.warn("Thread pool for metacat refresh did not terminate");
                    }
                }
            } catch (InterruptedException ie) {
                // (Re-)Cancel if current thread also interrupted
                executorService.shutdownNow();
                // Preserve interrupt status
                Thread.currentThread().interrupt();
            }
        }
    }

    private void deleteUnmarkedEntities(final List<QualifiedName> qNames,
                                        final List<QualifiedName> excludeQualifiedNames) {
        log.info("Start: Delete unmarked entities");
        //
        // get unmarked qualified names
        // check if it not exists
        // delete
        //
        elasticSearchUtil.refresh();
        final MetacatRequestContext context = MetacatRequestContext.builder().userName("admin").
            clientAppName("metacat-refresh")
            .apiUri("esRefresh")
            .scheme("internal").build();


        final List<DatabaseDto> unmarkedDatabaseDtos = elasticSearchUtil
            .getQualifiedNamesByMarkerByNames("database", qNames, refreshMarker, excludeQualifiedNames,
                DatabaseDto.class);
        if (!unmarkedDatabaseDtos.isEmpty()) {
            if (unmarkedDatabaseDtos.size() <= config.getElasticSearchThresholdUnmarkedDatabasesDelete()) {
                log.info("Start: Delete unmarked databases({})", unmarkedDatabaseDtos.size());
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
                    elasticSearchUtil.softDelete("database", deleteDatabaseNames, context);
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
                qNames, refreshMarker, excludeQualifiedNames, TableDto.class);
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
                                new MetacatDeleteTablePostEvent(tableDto.getName(), context, this, tableDto)
                            );
                        }
                    );
                }
                log.info("End: Delete unmarked tables({})", unmarkedTableDtos.size());
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

    @SuppressWarnings("checkstyle:methodname")
    private ListenableFuture<Void> _processCatalogs(final List<String> catalogNames) {
        log.info("Start: Full refresh of catalogs: {}", catalogNames);
        final List<ListenableFuture<CatalogDto>> getCatalogFutures = catalogNames.stream()
            .map(catalogName -> service.submit(() -> {
                CatalogDto result = null;
                try {
                    result = getCatalog(catalogName);
                } catch (Exception e) {
                    log.error("Failed to retrieve catalog: {}", catalogName);
                    elasticSearchUtil.log("ElasticSearchRefresh.getCatalog",
                        ElasticSearchDoc.Type.catalog.name(), catalogName, null,
                        e.getMessage(), e, true);
                }
                return result;
            }))
            .collect(Collectors.toList());
        return Futures.transformAsync(Futures.successfulAsList(getCatalogFutures),
            input -> {
                final List<ListenableFuture<Void>> processCatalogFutures = input.stream().filter(NOT_NULL).map(
                    catalogDto -> {
                        final List<QualifiedName> databaseNames = getDatabaseNamesToRefresh(catalogDto);
                        return _processDatabases(catalogDto.getName(), databaseNames);
                    }).filter(NOT_NULL).collect(Collectors.toList());
                return Futures.transform(Futures.successfulAsList(processCatalogFutures),
                    Functions.constant(null), defaultService);
            }, defaultService);
    }

    private List<QualifiedName> getDatabaseNamesToRefresh(final CatalogDto catalogDto) {
        List<QualifiedName> result = null;
        if (!config.getElasticSearchRefreshIncludeDatabases().isEmpty()) {
            result = config.getElasticSearchRefreshIncludeDatabases().stream()
                .filter(q -> catalogDto.getName().getCatalogName().equals(q.getCatalogName()))
                .collect(Collectors.toList());
        } else {
            result = catalogDto.getDatabases().stream()
                .map(n -> QualifiedName.ofDatabase(catalogDto.getName().getCatalogName(), n))
                .collect(Collectors.toList());
        }
        if (!config.getElasticSearchRefreshExcludeQualifiedNames().isEmpty()) {
            result.removeAll(config.getElasticSearchRefreshExcludeQualifiedNames());
        }
        return result;
    }

    private List<String> getCatalogNamesToRefresh() {
        List<String> result = null;
        if (!Strings.isNullOrEmpty(config.getElasticSearchRefreshIncludeCatalogs())) {
            result = Splitter.on(',').omitEmptyStrings().trimResults()
                .splitToList(config.getElasticSearchRefreshIncludeCatalogs());
        } else {
            result = getCatalogNames();
        }
        return result;
    }

    /**
     * Process the list of databases.
     *
     * @param catalogName   catalog name
     * @param databaseNames database names
     * @return future
     */
    @SuppressWarnings("checkstyle:methodname")
    private ListenableFuture<Void> _processDatabases(final QualifiedName catalogName,
                                                     final List<QualifiedName> databaseNames) {
        ListenableFuture<Void> resultFuture = null;
        log.info("Full refresh of catalog {} for databases({}): {}", catalogName, databaseNames.size(), databaseNames);
        final List<ListenableFuture<DatabaseDto>> getDatabaseFutures = databaseNames.stream()
            .map(databaseName -> service.submit(() -> {
                DatabaseDto result = null;
                try {
                    result = getDatabase(databaseName);
                } catch (Exception e) {
                    log.error("Failed to retrieve database: {}", databaseName);
                    elasticSearchUtil.log("ElasticSearchRefresh.getDatabase",
                        ElasticSearchDoc.Type.database.name(),
                        databaseName.toString(), null, e.getMessage(), e, true);
                }
                return result;
            }))
            .collect(Collectors.toList());

        if (getDatabaseFutures != null && !getDatabaseFutures.isEmpty()) {
            resultFuture = Futures.transformAsync(Futures.successfulAsList(getDatabaseFutures),
                input -> {
                    final ListenableFuture<Void> processDatabaseFuture = indexDatabaseDtos(catalogName, input);
                    final List<ListenableFuture<Void>> processDatabaseFutures = input.stream().filter(NOT_NULL)
                        .map(databaseDto -> {
                            final List<QualifiedName> tableNames = databaseDto.getTables().stream()
                                .map(s -> QualifiedName.ofTable(databaseDto.getName().getCatalogName(),
                                    databaseDto.getName().getDatabaseName(), s))
                                .collect(Collectors.toList());
                            log.info("Full refresh of database {} for tables({}): {}",
                                databaseDto.getName(),
                                databaseDto.getTables().size(), databaseDto.getTables());
                            return processTables(databaseDto.getName(), tableNames);
                        }).filter(NOT_NULL).collect(Collectors.toList());
                    processDatabaseFutures.add(processDatabaseFuture);
                    return Futures.transform(Futures.successfulAsList(processDatabaseFutures),
                        Functions.constant(null), defaultService);
                }, defaultService);
        }

        return resultFuture;
    }

    /**
     * Save all databases to index it in elastic search.
     *
     * @param catalogName catalog name
     * @param dtos        database dtos
     * @return future
     */
    private ListenableFuture<Void> indexDatabaseDtos(final QualifiedName catalogName, final List<DatabaseDto> dtos) {
        return esService.submit(() -> {
            final List<ElasticSearchDoc> docs = dtos.stream()
                .filter(dto -> dto != null)
                .map(dto -> new ElasticSearchDoc(dto.getName().toString(), dto, "admin", false, refreshMarkerText))
                .collect(Collectors.toList());
            log.info("Saving databases for catalog: {}", catalogName);
            elasticSearchUtil.save(ElasticSearchDoc.Type.database.name(), docs);
            return null;
        });
    }

    /**
     * Process the list of tables in batches.
     *
     * @param databaseName database name
     * @param tableNames   table names
     * @return A future containing the tasks
     */
    private ListenableFuture<Void> processTables(final QualifiedName databaseName,
                                                 final List<QualifiedName> tableNames) {
        final List<List<QualifiedName>> tableNamesBatches = Lists.partition(tableNames, 500);
        final List<ListenableFuture<Void>> processTablesBatchFutures = tableNamesBatches.stream().map(
            subTableNames -> _processTables(databaseName, subTableNames)).collect(Collectors.toList());

        return Futures.transform(Futures.successfulAsList(processTablesBatchFutures),
            Functions.constant(null), defaultService);
    }

    @SuppressWarnings("checkstyle:methodname")
    private ListenableFuture<Void> _processTables(final QualifiedName databaseName,
                                                  final List<QualifiedName> tableNames) {
        final List<ListenableFuture<Optional<TableDto>>> getTableFutures = tableNames.stream()
            .map(tableName -> service.submit(() -> {
                Optional<TableDto> result = null;
                try {
                    result = getTable(tableName);
                } catch (Exception e) {
                    log.error("Failed to retrieve table: {}", tableName);
                    elasticSearchUtil.log("ElasticSearchRefresh.getTable",
                        ElasticSearchDoc.Type.table.name(),
                        tableName.toString(), null, e.getMessage(), e, true);
                }
                return result;
            }))
            .collect(Collectors.toList());

        return Futures.transformAsync(Futures.successfulAsList(getTableFutures),
            input -> indexTableDtos(databaseName, input), defaultService);
    }

    /**
     * Save all tables to index it in elastic search.
     *
     * @param databaseName database name
     * @param dtos         table dtos
     * @return future
     */
    private ListenableFuture<Void> indexTableDtos(final QualifiedName databaseName,
                                                  final List<Optional<TableDto>> dtos) {
        return esService.submit(() -> {
            final List<ElasticSearchDoc> docs = dtos.stream().filter(dto -> dto != null && dto.isPresent()).map(
                tableDtoOptional -> {
                    final TableDto dto = tableDtoOptional.get();
                    final String userName = dto.getAudit() != null ? dto.getAudit().getCreatedBy() : "admin";
                    return new ElasticSearchDoc(dto.getName().toString(), dto, userName, false, refreshMarkerText);
                }).collect(Collectors.toList());
            log.info("Saving tables for database: {}", databaseName);
            elasticSearchUtil.save(ElasticSearchDoc.Type.table.name(), docs);
            return null;
        });
    }

    /**
     * Save all tables to index it in elastic search.
     *
     * @param tableName database name
     * @param dtos      partition dtos
     * @return future
     */
    private ListenableFuture<Void> indexPartitionDtos(final QualifiedName tableName, final List<PartitionDto> dtos) {
        return esService.submit(() -> {
            final List<ElasticSearchDoc> docs = dtos.stream().filter(dto -> dto != null).map(
                dto -> {
                    final String userName = dto.getAudit() != null ? dto.getAudit().getCreatedBy() : "admin";
                    return new ElasticSearchDoc(dto.getName().toString(), dto, userName, false, refreshMarkerText);
                }).collect(Collectors.toList());
            log.info("Saving partitions for tableName: {}", tableName);
            elasticSearchUtil.save(ElasticSearchDoc.Type.partition.name(), docs);
            return null;
        });
    }

    protected List<String> getCatalogNames() {
        return catalogService.getCatalogNames().stream().map(CatalogMappingDto::getCatalogName).collect(
            Collectors.toList());
    }

    protected CatalogDto getCatalog(final String catalogName) {
        return catalogService.get(QualifiedName.ofCatalog(catalogName));
    }

    protected DatabaseDto getDatabase(final QualifiedName databaseName) {
        return databaseService.get(databaseName,
            GetDatabaseServiceParameters.builder()
                .disableOnReadMetadataIntercetor(false)
                .includeTableNames(true)
                .includeUserMetadata(true)
                .build());
    }

    protected Optional<TableDto> getTable(final QualifiedName tableName) {
        return tableService.get(tableName, GetTableServiceParameters.builder()
            .disableOnReadMetadataIntercetor(false)
            .includeInfo(true)
            .includeDefinitionMetadata(true)
            .includeDataMetadata(true)
            .build());
    }
}
