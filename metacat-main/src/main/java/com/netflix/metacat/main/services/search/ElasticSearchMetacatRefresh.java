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

import com.facebook.presto.spi.NotFoundException;
import com.facebook.presto.spi.Pageable;
import com.facebook.presto.spi.Sort;
import com.facebook.presto.spi.SortOrder;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.*;
import com.netflix.metacat.common.MetacatContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.CatalogDto;
import com.netflix.metacat.common.dto.CatalogMappingDto;
import com.netflix.metacat.common.dto.DatabaseDto;
import com.netflix.metacat.common.dto.HasMetadata;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.exception.MetacatNotFoundException;
import com.netflix.metacat.common.monitoring.CounterWrapper;
import com.netflix.metacat.common.monitoring.TimerWrapper;
import com.netflix.metacat.common.server.Config;
import com.netflix.metacat.common.usermetadata.TagService;
import com.netflix.metacat.common.usermetadata.UserMetadataService;
import com.netflix.metacat.common.util.MetacatContextManager;
import com.netflix.metacat.main.services.CatalogService;
import com.netflix.metacat.main.services.DatabaseService;
import com.netflix.metacat.main.services.PartitionService;
import com.netflix.metacat.main.services.TableService;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.netflix.metacat.main.services.search.ElasticSearchDoc.Type.catalog;
import static com.netflix.metacat.main.services.search.ElasticSearchDoc.Type.database;
import static com.netflix.metacat.main.services.search.ElasticSearchDoc.Type.partition;
import static com.netflix.metacat.main.services.search.ElasticSearchDoc.Type.table;

/**
 * This class does a refresh of all the metadata entities from original data sources to elastic search
 * Created by amajumdar on 8/20/15.
 */
public class ElasticSearchMetacatRefresh {
    private static final Logger log = LoggerFactory.getLogger(ElasticSearchMetacatRefresh.class);
    private static AtomicBoolean isElasticSearchMetacatRefreshAlreadyRunning = new AtomicBoolean(false);
    private static final Predicate<Object> notNull = o -> o != null;
    private Instant refreshMarker;
    private String refreshMarkerText;
    @Inject
    CatalogService catalogService;
    @Inject
    Config config;
    @Inject
    DatabaseService databaseService;
    @Inject
    TableService tableService;
    @Inject
    PartitionService partitionService;
    @Inject
    ElasticSearchUtil elasticSearchUtil;
    @Inject
    UserMetadataService userMetadataService;
    @Inject
    TagService tagService;
    //  Fixed thread pool
    ListeningExecutorService service;
    ListeningExecutorService esService;

    public void process(){
        List<String> catalogNames = getCatalogNamesToRefresh();
        List<QualifiedName> qNames = catalogNames.stream()
                .map(QualifiedName::ofCatalog).collect(Collectors.toList());
        _process(qNames, () -> _processCatalogs(catalogNames), "process", true, 1000);
    }

    public void processCatalogs(List<String> catalogNames){
        List<QualifiedName> qNames = catalogNames.stream()
                .map(QualifiedName::ofCatalog).collect(Collectors.toList());
        _process( qNames, () -> _processCatalogs(catalogNames), "processCatalogs", true, 1000);
    }

    public void processDatabases(String catalogName, List<String> databaseNames){
        List<QualifiedName> qNames = databaseNames.stream()
                .map(s -> QualifiedName.ofDatabase(catalogName, s)).collect(Collectors.toList());
        _process(qNames, () -> _processDatabases(QualifiedName.ofCatalog(catalogName), qNames), "processDatabases", true, 1000);
    }

    public void processPartitions(List<QualifiedName> qNames){
        if( qNames == null || qNames.isEmpty()) {
            List<String> catalogNames = Splitter.on(',').omitEmptyStrings().trimResults()
                    .splitToList(config.getElasticSearchRefreshPartitionsIncludeCatalogs());
            qNames = catalogNames.stream()
                    .map(QualifiedName::ofCatalog).collect(Collectors.toList());
        }
        final List<QualifiedName> qualifiedNames = qNames;
        _process( qualifiedNames, () -> _processPartitions(qualifiedNames), "processPartitions", false, 500);
    }

    private ListenableFuture<Void> _processPartitions(List<QualifiedName> qNames) {
        final List<QualifiedName> excludeQualifiedNames = config.getElasticSearchRefreshExcludeQualifiedNames();
        List<String> tables = elasticSearchUtil.getTableIdsByCatalogs(table.name(), qNames, excludeQualifiedNames);
        List<ListenableFuture<ListenableFuture<Void>>> futures = tables.stream().map(s -> service.submit(() -> {
            QualifiedName tableName = QualifiedName.fromString(s, false);
            List<ListenableFuture<Void>> indexFutures = Lists.newArrayList();
            int offset = 0;
            int count;
            Sort sort = null;
            if ("s3".equals(tableName.getCatalogName()) || "aegisthus".equals(tableName.getCatalogName())) {
                sort = new Sort("id", SortOrder.ASC);
            } else {
                sort = new Sort("part_id", SortOrder.ASC);
            }
            Pageable pageable = new Pageable(10000, offset);
            do {
                List<PartitionDto> partitionDtos = partitionService.list(tableName, null, null, sort, pageable, true,
                        true, true);
                count = partitionDtos.size();
                if (!partitionDtos.isEmpty()) {
                    List<List<PartitionDto>> partitionedPartitionDtos = Lists.partition(partitionDtos, 1000);
                    partitionedPartitionDtos.forEach(
                            subPartitionsDtos -> indexFutures.add(indexPartitionDtos(tableName, subPartitionsDtos)));
                    offset = offset + count;
                    pageable.setOffset( offset);
                }
            } while (count == 10000);
            return Futures.transform(Futures.successfulAsList(indexFutures), Functions.constant((Void) null));
        })).collect(Collectors.toList());
        ListenableFuture<Void> processPartitionsFuture = Futures.transformAsync(Futures.successfulAsList(futures),
                input -> {
                    List<ListenableFuture<Void>> inputFuturesWithoutNulls = input.stream().filter(notNull).collect(Collectors.toList());
                    return Futures.transform(Futures.successfulAsList(inputFuturesWithoutNulls), Functions.constant(null));
                });
        return Futures.transformAsync(processPartitionsFuture, new AsyncFunction<Void, Void>() {
            @Override
            public ListenableFuture<Void> apply(@Nullable Void input) throws Exception {
                elasticSearchUtil.refresh();
                List<ListenableFuture<Void>> cleanUpFutures = tables.stream()
                        .map( s -> service.submit(() -> partitionsCleanUp( QualifiedName.fromString(s, false), excludeQualifiedNames)))
                        .collect(Collectors.toList());
                return Futures.transform(Futures.successfulAsList(cleanUpFutures), Functions.constant(null));
            }
        });
    }

    private Void partitionsCleanUp(QualifiedName tableName, List<QualifiedName> excludeQualifiedNames) {
        try {
            final List<PartitionDto> unmarkedPartitionDtos = elasticSearchUtil.getQualifiedNamesByMarkerByNames(
                    ElasticSearchDoc.Type.partition.name(),
                    Lists.newArrayList(tableName), refreshMarker, excludeQualifiedNames, PartitionDto.class);
            if (!unmarkedPartitionDtos.isEmpty()) {
                log.info("Start deleting unmarked partitions({}) for table {}", unmarkedPartitionDtos.size(), tableName.toString());
                try {
                    final List<String> unmarkedPartitionNames = unmarkedPartitionDtos.stream()
                            .map(p -> p.getDefinitionName().getPartitionName()).collect(Collectors.toList());
                    final Set<String> existingUnmarkedPartitionNames = Sets.newHashSet(
                            partitionService.getPartitionKeys(tableName, null, unmarkedPartitionNames, null, null));
                    final List<String> partitionIds = unmarkedPartitionDtos.stream()
                            .filter(p -> !existingUnmarkedPartitionNames.contains(p.getDefinitionName().getPartitionName()))
                            .map(p -> p.getDefinitionName().toString()).collect(Collectors.toList());
                    if (!partitionIds.isEmpty()) {
                        log.info("Deleting unused partitions({}) for table {}:{}", partitionIds.size(), tableName.toString(), partitionIds);
                        elasticSearchUtil.delete(ElasticSearchDoc.Type.partition.name(), partitionIds);
                        final List<HasMetadata> deletePartitionDtos = unmarkedPartitionDtos.stream()
                                .filter(p -> !existingUnmarkedPartitionNames.contains(p.getDefinitionName().getPartitionName()))
                                .collect(Collectors.toList());
                        userMetadataService.deleteMetadatas("admin", deletePartitionDtos);
                    }
                } catch(Exception e){
                    log.warn("Failed deleting the unmarked partitions for table {}", tableName.toString());
                }
                log.info("End deleting unmarked partitions for table {}", tableName.toString());
            }
        } catch(Exception e){
            log.warn("Failed getting the unmarked partitions for table {}", tableName.toString());
        }
        return null;
    }

    private static ExecutorService newFixedThreadPool(int nThreads, String threadFactoryName, int queueSize) {
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

    private void _process(List<QualifiedName> qNames, Supplier<ListenableFuture<Void>> supplier, String requestName, boolean delete, int queueSize){
        if( isElasticSearchMetacatRefreshAlreadyRunning.compareAndSet( false, true)) {
            TimerWrapper timer = TimerWrapper.createStarted("dse.metacat.timer.ElasticSearchMetacatRefresh." + requestName);
            try {
                log.info("Start: Full refresh of metacat index in elastic search. Processing {} ...", qNames);
                MetacatContext context = new MetacatContext( "admin", "elasticSearchRefresher", null, null, null);
                MetacatContextManager.setContext(context);
                refreshMarker = Instant.now();
                refreshMarkerText = refreshMarker.toString();
                service = MoreExecutors.listeningDecorator(newFixedThreadPool(10, "elasticsearch-refresher-%d", queueSize));
                esService = MoreExecutors.listeningDecorator(newFixedThreadPool(5, "elasticsearch-refresher-es-%d", queueSize));
                supplier.get().get(24, TimeUnit.HOURS);
                log.info("End: Full refresh of metacat index in elastic search");
                if( delete) {
                    deleteUnmarkedEntities(qNames, config.getElasticSearchRefreshExcludeQualifiedNames());
                }
            } catch (Exception e) {
                log.error("Full refresh of metacat index failed", e);
                CounterWrapper.incrementCounter("dse.metacat.elasticSearchMetacatRefreshFailureCount");
            } finally {
                try {
                    shutdown(service);
                    shutdown(esService);
                } finally {
                    isElasticSearchMetacatRefreshAlreadyRunning.set(false);
                    log.info("### Time taken to complete {} is {} ms", requestName, timer.stop());
                }
            }

        } else {
            log.info("Full refresh of metacat index is already running.");
            CounterWrapper.incrementCounter("dse.metacat.elasticSearchMetacatRefreshAlreadyRunning");
        }
    }

    private void shutdown(ListeningExecutorService executorService) {
        if( executorService != null){
            executorService.shutdown();
            try {
                // Wait a while for existing tasks to terminate
                if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                    executorService.shutdownNow(); // Cancel currently executing tasks
                    // Wait a while for tasks to respond to being cancelled
                    if (!executorService.awaitTermination(60, TimeUnit.SECONDS))
                        log.warn("Thread pool for metacat refresh did not terminate");
                }
            } catch (InterruptedException ie) {
                // (Re-)Cancel if current thread also interrupted
                executorService.shutdownNow();
                // Preserve interrupt status
                Thread.currentThread().interrupt();
            }
        }
    }

    private void deleteUnmarkedEntities(List<QualifiedName> qNames, List<QualifiedName> excludeQualifiedNames) {
        log.info("Start: Delete unmarked entities");
        //
        // get unmarked qualified names
        // check if it not exists
        // delete
        //
        elasticSearchUtil.refresh();
        MetacatContext context = new MetacatContext("admin", "metacat-refresh", null, null, null);

        List<DatabaseDto> unmarkedDatabaseDtos = elasticSearchUtil.getQualifiedNamesByMarkerByNames("database", qNames, refreshMarker, excludeQualifiedNames, DatabaseDto.class);
        if( !unmarkedDatabaseDtos.isEmpty()) {
            if(unmarkedDatabaseDtos.size() <= config.getElasticSearchThresholdUnmarkedDatabasesDelete()) {
                log.info("Start: Delete unmarked databases({})", unmarkedDatabaseDtos.size());
                List<String> unmarkedDatabaseNames = Lists.newArrayList();
                List<DatabaseDto> deleteDatabaseDtos = unmarkedDatabaseDtos.stream().filter(databaseDto -> {
                    boolean result = false;
                    try {
                        unmarkedDatabaseNames.add(databaseDto.getName().toString());
                        DatabaseDto dto = databaseService.get(databaseDto.getName(), false);
                        if (dto == null) {
                            result = true;
                        }
                    } catch (NotFoundException | MetacatNotFoundException ignored) {
                        result = true;
                    } catch (Exception ignored) {}
                    return result;
                }).collect(Collectors.toList());
                log.info("Unmarked databases({}): {}", unmarkedDatabaseNames.size(), unmarkedDatabaseNames);
                log.info("Deleting databases({})", deleteDatabaseDtos.size());
                if (!deleteDatabaseDtos.isEmpty()) {
                    List<QualifiedName> deleteDatabaseQualifiedNames = deleteDatabaseDtos.stream()
                            .map(DatabaseDto::getName)
                            .collect(Collectors.toList());
                    List<String> deleteDatabaseNames = deleteDatabaseQualifiedNames.stream().map(
                            QualifiedName::toString).collect(Collectors.toList());
                    log.info("Deleting databases({}): {}", deleteDatabaseNames.size(), deleteDatabaseNames);
                    userMetadataService.deleteDefinitionMetadatas(deleteDatabaseQualifiedNames);
                    elasticSearchUtil.softDelete("database", deleteDatabaseNames, context);
                }
                log.info("End: Delete unmarked databases({})", unmarkedDatabaseDtos.size());
            }else {
                log.info("Count of unmarked databases({}) is more than the threshold {}", unmarkedDatabaseDtos.size(), config.getElasticSearchThresholdUnmarkedDatabasesDelete());
                CounterWrapper.incrementCounter("dse.metacat.counter.unmarked.databases.threshold.crossed");
            }
        }

        List<TableDto> unmarkedTableDtos = elasticSearchUtil.getQualifiedNamesByMarkerByNames("table", qNames, refreshMarker, excludeQualifiedNames, TableDto.class);
        if( !unmarkedTableDtos.isEmpty() ) {
            if(unmarkedTableDtos.size() <= config.getElasticSearchThresholdUnmarkedTablesDelete()) {
                log.info("Start: Delete unmarked tables({})", unmarkedTableDtos.size());
                List<String> unmarkedTableNames = Lists.newArrayList();
                List<TableDto> deleteTableDtos = unmarkedTableDtos.stream().filter(tableDto -> {
                    boolean result = false;
                    try {
                        unmarkedTableNames.add(tableDto.getName().toString());
                        Optional<TableDto> dto = tableService.get(tableDto.getName(), false);
                        if (!dto.isPresent()) {
                            result = true;
                        }
                    } catch (NotFoundException | MetacatNotFoundException ignored) {
                        result = true;
                    } catch (Exception ignored) {}
                    return result;
                }).collect(Collectors.toList());
                log.info("Unmarked tables({}): {}", unmarkedTableNames.size(), unmarkedTableNames);
                log.info("Deleting tables({}): {}", deleteTableDtos.size());
                if (!deleteTableDtos.isEmpty()) {
                    List<String> deleteTableNames = deleteTableDtos.stream().map(
                            dto -> dto.getName().toString()).collect(Collectors.toList());
                    log.info("Deleting tables({}): {}", deleteTableNames.size(), deleteTableNames);
                    userMetadataService.deleteMetadatas("admin", Lists.newArrayList(deleteTableDtos));
                    elasticSearchUtil.softDelete("table", deleteTableNames, context);
                    deleteTableDtos.forEach(tableDto -> tagService.delete( tableDto.getName(), false));
                }
                log.info("End: Delete unmarked tables({})", unmarkedTableDtos.size());
            } else {
                log.info("Count of unmarked tables({}) is more than the threshold {}", unmarkedTableDtos.size(), config.getElasticSearchThresholdUnmarkedTablesDelete());
                CounterWrapper.incrementCounter("dse.metacat.counter.unmarked.tables.threshold.crossed");

            }
        }
        log.info("End: Delete unmarked entities");
    }

    private ListenableFuture<Void> _processCatalogs(List<String> catalogNames){
        log.info("Start: Full refresh of catalogs: {}", catalogNames);
        List<ListenableFuture<CatalogDto>> getCatalogFutures = catalogNames.stream()
                .map(catalogName -> service.submit(() -> {
                    CatalogDto result = null;
                    try {
                        result = getCatalog(catalogName);
                    } catch (Exception e) {
                        log.error("Failed to retrieve catalog: {}", catalogName);
                        elasticSearchUtil.log("ElasticSearchMetacatRefresh.getCatalog", catalog.name(), catalogName, null, e.getMessage(), e, true);
                    }
                    return result;
                }))
                .collect(Collectors.toList());
        return Futures.transformAsync( Futures.successfulAsList(getCatalogFutures),
                input -> {
                    List<ListenableFuture<Void>> processCatalogFutures = input.stream().filter(notNull).map(
                            catalogDto -> {
                                List<QualifiedName> databaseNames = getDatabaseNamesToRefresh(catalogDto);
                                return _processDatabases(catalogDto.getName(), databaseNames);
                            }).filter(notNull).collect(Collectors.toList());
                    return Futures.transform(Futures.successfulAsList(processCatalogFutures), Functions.constant(null));
                });
    }

    private List<QualifiedName> getDatabaseNamesToRefresh(CatalogDto catalogDto) {
        List<QualifiedName> result = null;
        if(!config.getElasticSearchRefreshIncludeDatabases().isEmpty()){
            result = config.getElasticSearchRefreshIncludeDatabases().stream()
                    .filter(q -> catalogDto.getName().getCatalogName().equals(q.getCatalogName()))
                    .collect(Collectors.toList());
        } else {
            result = catalogDto.getDatabases().stream()
                    .map(n -> QualifiedName.ofDatabase( catalogDto.getName().getCatalogName(), n))
                    .collect(Collectors.toList());
        }
        if(!config.getElasticSearchRefreshExcludeQualifiedNames().isEmpty()){
            result.removeAll(config.getElasticSearchRefreshExcludeQualifiedNames());
        }
        return result;
    }

    private List<String> getCatalogNamesToRefresh() {
        List<String> result = null;
        if(!Strings.isNullOrEmpty(config.getElasticSearchRefreshIncludeCatalogs())){
            result = Splitter.on(',').omitEmptyStrings().trimResults().splitToList(config.getElasticSearchRefreshIncludeCatalogs());
        } else {
            result = getCatalogNames();
        }
        return result;
    }

    /**
     * Process the list of databases
     * @param catalogName catalog name
     * @param databaseNames database names
     * @return future
     */
    private ListenableFuture<Void> _processDatabases(QualifiedName catalogName, List<QualifiedName> databaseNames){
        ListenableFuture<Void> resultFuture = null;
        log.info("Full refresh of catalog {} for databases({}): {}", catalogName, databaseNames.size(), databaseNames);
        List<ListenableFuture<DatabaseDto>> getDatabaseFutures = databaseNames.stream()
                .map(databaseName -> service.submit(() -> {
                    DatabaseDto result = null;
                    try {
                        result = getDatabase( databaseName);
                    } catch (Exception e) {
                        log.error("Failed to retrieve database: {}", databaseName);
                        elasticSearchUtil.log("ElasticSearchMetacatRefresh.getDatabase", database.name(), databaseName.toString(), null, e.getMessage(), e, true);
                    }
                    return result;
                }))
                .collect(Collectors.toList());

        if( getDatabaseFutures != null && !getDatabaseFutures.isEmpty()) {
            resultFuture =  Futures.transformAsync(Futures.successfulAsList(getDatabaseFutures),
                    input -> {
                        ListenableFuture<Void> processDatabaseFuture = indexDatabaseDtos(catalogName, input);
                        List<ListenableFuture<Void>> processDatabaseFutures = input.stream().filter(notNull)
                                .map(databaseDto -> {
                                    List<QualifiedName> tableNames = databaseDto.getTables().stream()
                                            .map(s -> QualifiedName.ofTable(databaseDto.getName().getCatalogName(),
                                                    databaseDto.getName().getDatabaseName(), s))
                                            .collect(Collectors.toList());
                                    log.info("Full refresh of database {} for tables({}): {}", databaseDto.getName().toString(), databaseDto.getTables().size(), databaseDto.getTables());
                                    return processTables(databaseDto.getName(), tableNames);
                                }).filter(notNull).collect(Collectors.toList());
                        processDatabaseFutures.add(processDatabaseFuture);
                        return Futures.transform(Futures.successfulAsList(processDatabaseFutures), Functions.constant(null));
                    });
         }

        return resultFuture;
    }

    /**
     * Save all databases to index it in elastic search
     * @param catalogName catalog name
     * @param dtos database dtos
     * @return future
     */
    private ListenableFuture<Void> indexDatabaseDtos(QualifiedName catalogName, List<DatabaseDto> dtos){
        return esService.submit(() -> {
            List<ElasticSearchDoc> docs = dtos.stream()
                    .filter(dto -> dto!=null)
                    .map( dto -> new ElasticSearchDoc( dto.getName().toString(), dto, "admin", false, refreshMarkerText))
                    .collect(Collectors.toList());
            log.info("Saving databases for catalog: {}", catalogName);
            elasticSearchUtil.save(database.name(), docs);
            return null;
        });
    }

    /**
     * Process the list of tables in batches
     * @param databaseName database name
     * @param tableNames table names
     * @return A future containing the tasks
     */
    private ListenableFuture<Void> processTables(QualifiedName databaseName, List<QualifiedName> tableNames){
        List<List<QualifiedName>> tableNamesBatches = Lists.partition( tableNames, 500);
        List<ListenableFuture<Void>> processTablesBatchFutures = tableNamesBatches.stream().map(
                subTableNames -> _processTables( databaseName, subTableNames)).collect(Collectors.toList());

        return Futures.transform(Futures.successfulAsList(processTablesBatchFutures),Functions.constant(null));
    }

    private ListenableFuture<Void> _processTables(QualifiedName databaseName, List<QualifiedName> tableNames){
        List<ListenableFuture<Optional<TableDto>>> getTableFutures = tableNames.stream()
                .map(tableName -> service.submit(() -> {
                    Optional<TableDto> result = null;
                    try {
                        result = getTable( tableName);
                    } catch (Exception e) {
                        log.error("Failed to retrieve table: {}", tableName);
                        elasticSearchUtil.log("ElasticSearchMetacatRefresh.getTable", table.name(), tableName.toString(), null, e.getMessage(), e, true);
                    }
                    return result;
                }))
                .collect(Collectors.toList());

        return Futures.transformAsync(Futures.successfulAsList(getTableFutures),
                input -> indexTableDtos( databaseName, input));
    }

    /**
     * Save all tables to index it in elastic search
     * @param databaseName database name
     * @param dtos table dtos
     * @return future
     */
    private ListenableFuture<Void> indexTableDtos(QualifiedName databaseName, List<Optional<TableDto>> dtos){
        return esService.submit(() -> {
            List<ElasticSearchDoc> docs = dtos.stream().filter(dto -> dto!= null && dto.isPresent()).map(
                    tableDtoOptional -> {
                        TableDto dto = tableDtoOptional.get();
                        String userName = dto.getAudit() != null ? dto.getAudit().getCreatedBy()
                                : "admin";
                        return new ElasticSearchDoc(dto.getName().toString(), dto, userName, false, refreshMarkerText);
                    }).collect(Collectors.toList());
            log.info("Saving tables for database: {}", databaseName);
            elasticSearchUtil.save(table.name(), docs);
            return null;
        });
    }

    /**
     * Save all tables to index it in elastic search
     * @param tableName database name
     * @param dtos partition dtos
     * @return future
     */
    private ListenableFuture<Void> indexPartitionDtos(QualifiedName tableName, List<PartitionDto> dtos){
        return esService.submit(() -> {
            List<ElasticSearchDoc> docs = dtos.stream().filter(dto -> dto!=null).map(
                    dto -> {
                        String userName = dto.getAudit() != null ? dto.getAudit().getCreatedBy()
                                : "admin";
                        return new ElasticSearchDoc(dto.getName().toString(), dto, userName, false, refreshMarkerText);
                    }).collect(Collectors.toList());
            log.info("Saving partitions for tableName: {}", tableName);
            elasticSearchUtil.save(partition.name(), docs);
            return null;
        });
    }

    protected List<String> getCatalogNames() {
        return catalogService.getCatalogNames().stream().map(CatalogMappingDto::getCatalogName).collect(
                Collectors.toList());
    }

    protected CatalogDto getCatalog(String catalogName) {
        return catalogService.get(QualifiedName.ofCatalog(catalogName));
    }

    protected DatabaseDto getDatabase(QualifiedName databaseName) {
        return databaseService.get(databaseName, true);
    }

    protected Optional<TableDto> getTable(QualifiedName tableName) {
        return tableService.get(tableName, true);
    }
}
