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


package com.netflix.metacat.main.services;

import com.google.common.base.Functions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.CatalogDto;
import com.netflix.metacat.common.dto.DatabaseDto;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.server.monitoring.Metrics;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.common.server.util.MetacatContextManager;
import com.netflix.spectator.api.Registry;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.Instant;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
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
public class CatalogTraversal {
    private static final Predicate<Object> NOT_NULL = Objects::nonNull;
    private static AtomicBoolean isTraversalAlreadyRunning = new AtomicBoolean(false);

    private final CatalogTraversalServiceHelper catalogTraversalServiceHelper;
    private final List<CatalogTraversalAction> actions;
    private final Config config;

    private Registry registry;
    // Traversal state
    private Context context;
    //  Fixed thread pool
    private ListeningExecutorService service;
    private ListeningExecutorService actionService;
    private ExecutorService defaultService;


    /**
     * Constructor.
     *
     * @param config                            System config
     * @param catalogTraversalServiceHelper     Catalog service helper
     * @param registry                          registry of spectator
     */
    public CatalogTraversal(
        @Nonnull @NonNull final Config config,
        @Nonnull @NonNull final CatalogTraversalServiceHelper catalogTraversalServiceHelper,
        @Nonnull @NonNull final Registry registry
    ) {
        this.config = config;
        this.actions = Lists.newArrayList();
        this.catalogTraversalServiceHelper = catalogTraversalServiceHelper;
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
     * Adds the action handlers.
     * @param actionHandlers list of action handlers
     */
    public void addActions(final List<CatalogTraversalAction> actionHandlers) {
        this.actions.addAll(actionHandlers);
    }

    /**
     * Does a sweep across all catalogs to refresh the same data in elastic search.
     */
    public void process() {
        processCatalogs(catalogTraversalServiceHelper.getCatalogNames());
    }

    /**
     * Does a sweep across given catalogs to refresh the same data in elastic search.
     *
     * @param catalogNames catalog names
     */
    public void processCatalogs(final List<String> catalogNames) {
        final List<QualifiedName> qNames = catalogNames.stream()
            .map(QualifiedName::ofCatalog).collect(Collectors.toList());
        _process(qNames, () -> _processCatalogs(catalogNames), "processCatalogs", true, 1000);
    }

    @SuppressWarnings("checkstyle:methodname")
    private void _process(final List<QualifiedName> qNames, final Supplier<ListenableFuture<Void>> supplier,
                          final String requestName, final boolean delete, final int queueSize) {
        if (isTraversalAlreadyRunning.compareAndSet(false, true)) {
            final long start = registry.clock().wallTime();
            try {
                log.info("Start Traversal: Full catalog traversal. Processing {} ...", qNames);
                final MetacatRequestContext requestContext = MetacatRequestContext.builder()
                    .userName("admin")
                    .clientAppName("catalogTraversal")
                    .apiUri("catalogTraversal")
                    .scheme("internal")
                    .build();
                MetacatContextManager.setContext(requestContext);
                final Instant startInstant = Instant.now();
                context = new Context(startInstant.toString(), startInstant, qNames,
                    config.getElasticSearchRefreshExcludeQualifiedNames());
                service = MoreExecutors
                    .listeningDecorator(newFixedThreadPool(10, "catalog-traversal-%d", queueSize));
                actionService = MoreExecutors
                    .listeningDecorator(newFixedThreadPool(5, "catalog-traversal-action-service-%d", queueSize));
                defaultService = Executors.newSingleThreadExecutor();
                actions.forEach(a -> a.init(context));
                supplier.get().get(24, TimeUnit.HOURS);
                actions.forEach(a -> a.done(context));
                log.info("End Traversal: Full catalog traversal");
            } catch (Exception e) {
                log.error("Traversal: Full catalog traversal failed", e);
                registry.counter(registry.createId(Metrics.CounterCatalogTraversal.getMetricName())
                    .withTags(Metrics.tagStatusFailureMap)).increment();
            } finally {
                try {
                    shutdown(service);
                    shutdown(defaultService);
                } finally {
                    isTraversalAlreadyRunning.set(false);
                    final long duration = registry.clock().wallTime() - start;
                    this.registry.timer(Metrics.TimerCatalogTraversal.getMetricName()
                        + "." + requestName).record(duration, TimeUnit.MILLISECONDS);
                    log.info("### Time taken to complete {} is {} ms", requestName, duration);
                }
                actions.clear();
            }

        } else {
            log.info("Traversal: Full catalog traversal is already running.");
            registry.counter(registry.createId(Metrics.CounterCatalogTraversalAlreadyRunning.getMetricName()))
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
                        log.warn("Thread pool for metacat traversal did not terminate");
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

    @SuppressWarnings("checkstyle:methodname")
    private ListenableFuture<Void> _processCatalogs(final List<String> catalogNames) {
        log.info("Start: Full traversal of catalogs: {}", catalogNames);
        final List<List<String>> subCatalogNamesList = Lists.partition(catalogNames, 5);
        final List<ListenableFuture<Void>> futures =
            subCatalogNamesList.stream().map(this::_processSubCatalogList).collect(Collectors.toList());
        return Futures.transform(Futures.successfulAsList(futures), Functions.constant(null), defaultService);
    }

    @SuppressWarnings("checkstyle:methodname")
    private ListenableFuture<Void> _processSubCatalogList(final List<String> catalogNames) {
        log.info("Start: Full traversal of catalogs: {}", catalogNames);
        final List<ListenableFuture<CatalogDto>> getCatalogFutures = catalogNames.stream()
            .map(catalogName -> service.submit(() -> {
                CatalogDto result = null;
                try {
                    result = catalogTraversalServiceHelper.getCatalog(catalogName);
                } catch (Exception e) {
                    log.error("Traversal: Failed to retrieve catalog: {}", catalogName);
                    registry.counter(
                        registry.createId(Metrics.CounterCatalogTraversalCatalogReadFailed.getMetricName())
                            .withTag("catalogName", catalogName))
                        .increment();
                }
                return result;
            }))
            .collect(Collectors.toList());
        return Futures.transformAsync(Futures.successfulAsList(getCatalogFutures),
            input -> {
                final ListenableFuture<Void> processCatalogFuture = applyCatalogs(input);
                final List<ListenableFuture<Void>> processCatalogFutures = input.stream().filter(NOT_NULL).map(
                    catalogDto -> {
                        final List<QualifiedName> databaseNames = getDatabaseNamesToRefresh(catalogDto);
                        return _processDatabases(catalogDto, databaseNames);
                    }).filter(NOT_NULL).collect(Collectors.toList());
                processCatalogFutures.add(processCatalogFuture);
                return Futures.transform(Futures.successfulAsList(processCatalogFutures),
                    Functions.constant(null), defaultService);
            }, defaultService);
    }

    private List<QualifiedName> getDatabaseNamesToRefresh(final CatalogDto catalogDto) {
        final List<QualifiedName> result = catalogDto.getDatabases().stream()
                .map(n -> QualifiedName.ofDatabase(catalogDto.getName().getCatalogName(), n))
                .collect(Collectors.toList());
        final List<QualifiedName> excludeQNames = context.getExcludeQNames();
        if (excludeQNames != null && !excludeQNames.isEmpty()) {
            result.removeAll(excludeQNames);
        }
        return result;
    }

    /**
     * Process the list of databases.
     *
     * @param catalogDto   catalog dto
     * @param databaseNames database names
     * @return future
     */
    @SuppressWarnings("checkstyle:methodname")
    private ListenableFuture<Void> _processDatabases(final CatalogDto catalogDto,
                                                     final List<QualifiedName> databaseNames) {
        ListenableFuture<Void> resultFuture = null;
        final QualifiedName catalogName = catalogDto.getName();
        log.info("Traversal: Full traversal of catalog {} for databases({}): {}",
            catalogName, databaseNames.size(), databaseNames);
        final List<ListenableFuture<DatabaseDto>> getDatabaseFutures = databaseNames.stream()
            .map(databaseName -> service.submit(() -> {
                DatabaseDto result = null;
                try {
                    result = catalogTraversalServiceHelper.getDatabase(catalogDto, databaseName);
                } catch (Exception e) {
                    log.error("Traversal: Failed to retrieve database: {}", databaseName);
                    registry.counter(
                        registry.createId(Metrics.CounterCatalogTraversalDatabaseReadFailed.getMetricName())
                            .withTags(databaseName.parts()))
                        .increment();
                }
                return result;
            }))
            .collect(Collectors.toList());

        if (getDatabaseFutures != null && !getDatabaseFutures.isEmpty()) {
            resultFuture = Futures.transformAsync(Futures.successfulAsList(getDatabaseFutures),
                input -> {
                    final ListenableFuture<Void> processDatabaseFuture = applyDatabases(catalogName, input);
                    final List<ListenableFuture<Void>> processDatabaseFutures = input.stream().filter(NOT_NULL)
                        .map(databaseDto -> {
                            final List<QualifiedName> tableNames = databaseDto.getTables().stream()
                                .map(s -> QualifiedName.ofTable(databaseDto.getName().getCatalogName(),
                                    databaseDto.getName().getDatabaseName(), s))
                                .collect(Collectors.toList());
                            log.info("Traversal: Full traversal of database {} for tables({}): {}",
                                databaseDto.getName(),
                                databaseDto.getTables().size(), databaseDto.getTables());
                            return processTables(databaseDto, tableNames);
                        }).filter(NOT_NULL).collect(Collectors.toList());
                    processDatabaseFutures.add(processDatabaseFuture);
                    return Futures.transform(Futures.successfulAsList(processDatabaseFutures),
                        Functions.constant(null), defaultService);
                }, defaultService);
        }

        return resultFuture;
    }

    /**
     * Apply all catalogs to all registered actions.
     *
     * @param dtos catalog dtos
     * @return future
     */
    private ListenableFuture<Void> applyCatalogs(final List<CatalogDto> dtos) {
        final List<ListenableFuture<Void>> actionFutures = actions.stream()
            .map(a -> actionService.submit((Callable<Void>) () -> {
                a.applyCatalogs(context, dtos);
                return null;
            })).collect(Collectors.toList());
        return Futures.transform(Futures.successfulAsList(actionFutures),
            Functions.constant(null), defaultService);
    }
    /**
     * Apply all databases to all registered actions.
     *
     * @param name catalog name
     * @param dtos        database dtos
     * @return future
     */
    private ListenableFuture<Void> applyDatabases(final QualifiedName name, final List<DatabaseDto> dtos) {
        log.info("Traversal: Apply databases for catalog: {}", name);
        final List<ListenableFuture<Void>> actionFutures = actions.stream()
            .map(a -> actionService.submit((Callable<Void>) () -> {
                a.applyDatabases(context, dtos);
                return null;
            })).collect(Collectors.toList());
        return Futures.transform(Futures.successfulAsList(actionFutures),
            Functions.constant(null), defaultService);
    }

    /**
     * Apply all tables to all registered actions.
     *
     * @param name database Name
     * @param dtos        table dtos
     * @return future
     */
    private ListenableFuture<Void> applyTables(final QualifiedName name, final List<Optional<TableDto>> dtos) {
        log.info("Traversal: Apply tables for database: {}", name);
        final List<ListenableFuture<Void>> actionFutures = actions.stream()
            .map(a -> actionService.submit((Callable<Void>) () -> {
                a.applyTables(context, dtos);
                return null;
            })).collect(Collectors.toList());
        return Futures.transform(Futures.successfulAsList(actionFutures),
            Functions.constant(null), defaultService);
    }

    /**
     * Process the list of tables in batches.
     *
     * @param databaseDto database dto
     * @param tableNames   table names
     * @return A future containing the tasks
     */
    private ListenableFuture<Void> processTables(final DatabaseDto databaseDto,
                                                 final List<QualifiedName> tableNames) {
        final List<List<QualifiedName>> tableNamesBatches = Lists.partition(tableNames, 500);
        final List<ListenableFuture<Void>> processTablesBatchFutures = tableNamesBatches.stream().map(
            subTableNames -> _processTables(databaseDto, subTableNames)).collect(Collectors.toList());

        return Futures.transform(Futures.successfulAsList(processTablesBatchFutures),
            Functions.constant(null), defaultService);
    }

    @SuppressWarnings("checkstyle:methodname")
    private ListenableFuture<Void> _processTables(final DatabaseDto databaseDto,
                                                  final List<QualifiedName> tableNames) {
        final QualifiedName databaseName = databaseDto.getName();
        final List<ListenableFuture<Optional<TableDto>>> getTableFutures = tableNames.stream()
            .map(tableName -> service.submit(() -> {
                Optional<TableDto> result = null;
                try {
                    result = catalogTraversalServiceHelper.getTable(databaseDto, tableName);
                } catch (Exception e) {
                    log.error("Traversal: Failed to retrieve table: {}", tableName);
                    registry.counter(
                        registry.createId(Metrics.CounterCatalogTraversalTableReadFailed.getMetricName())
                            .withTags(tableName.parts()))
                        .increment();
                }
                return result;
            }))
            .collect(Collectors.toList());

        return Futures.transformAsync(Futures.successfulAsList(getTableFutures),
            input -> applyTables(databaseName, input), defaultService);
    }

    /**
     * Traversal context.
     */
    @Data
    @AllArgsConstructor
    public static class Context {
        private String runId;
        private Instant startInstant;
        private List<QualifiedName> qNames;
        private List<QualifiedName> excludeQNames;
    }
}
