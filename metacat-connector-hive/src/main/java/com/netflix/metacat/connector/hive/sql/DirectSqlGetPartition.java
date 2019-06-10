/*
 *  Copyright 2017 Netflix, Inc.
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
package com.netflix.metacat.connector.hive.sql;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext;
import com.netflix.metacat.common.server.connectors.exception.ConnectorException;
import com.netflix.metacat.common.server.connectors.model.AuditInfo;
import com.netflix.metacat.common.server.connectors.model.PartitionInfo;
import com.netflix.metacat.common.server.connectors.model.PartitionListRequest;
import com.netflix.metacat.common.server.connectors.model.StorageInfo;
import com.netflix.metacat.common.server.partition.parser.PartitionParser;
import com.netflix.metacat.common.server.partition.util.FilterPartition;
import com.netflix.metacat.common.server.partition.visitor.PartitionKeyParserEval;
import com.netflix.metacat.common.server.partition.visitor.PartitionParamParserEval;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.common.server.util.ThreadServiceManager;
import com.netflix.metacat.connector.hive.monitoring.HiveMetrics;
import com.netflix.metacat.connector.hive.util.HiveConfigConstants;
import com.netflix.metacat.connector.hive.util.HiveConnectorFastServiceMetric;
import com.netflix.metacat.connector.hive.util.PartitionFilterGenerator;
import com.netflix.spectator.api.Registry;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.SqlParameterValue;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.StringReader;
import java.sql.Types;
import java.time.Instant;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * This class makes direct sql calls to get partitions.
 *
 * @author zhenl
 * @since 1.0.0
 */
@Slf4j
@Transactional("hiveTxManager")
public class DirectSqlGetPartition {
    private static final String FIELD_DATE_CREATED = "dateCreated";
    private static final String FIELD_BATCHID = "batchid";
    private static final String AUDIT_DB = "audit";
    private static final Pattern AUDIT_TABLENAME_PATTERN = Pattern.compile(
        "(?<db>.*)__(?<table>.*)__audit(.*)$"
    );
    private static final String PARTITION_NAME = "name";
    private static final String PARTITION_URI = "uri";

    private final ThreadServiceManager threadServiceManager;
    private final Registry registry;
    private JdbcTemplate jdbcTemplate;
    private final HiveConnectorFastServiceMetric fastServiceMetric;
    private final String catalogName;
    private final Config config;
    private final Map<String, String> configuration;

    /**
     * Constructor.
     *
     * @param connectorContext     server context
     * @param threadServiceManager thread service manager
     * @param jdbcTemplate         JDBC template
     * @param fastServiceMetric    fast service metric
     */
    public DirectSqlGetPartition(
        final ConnectorContext connectorContext,
        final ThreadServiceManager threadServiceManager,
        @Qualifier("hiveReadJdbcTemplate") final JdbcTemplate jdbcTemplate,
        final HiveConnectorFastServiceMetric fastServiceMetric
    ) {
        this.catalogName = connectorContext.getCatalogName();
        this.threadServiceManager = threadServiceManager;
        this.registry = connectorContext.getRegistry();
        this.config = connectorContext.getConfig();
        this.jdbcTemplate = jdbcTemplate;
        this.fastServiceMetric = fastServiceMetric;
        configuration = connectorContext.getConfiguration();
    }

    /**
     * Number of partitions for the given table.
     *
     * @param requestContext request context
     * @param tableName      tableName
     * @return Number of partitions
     */
    @Transactional(readOnly = true)
    public int getPartitionCount(
        final ConnectorRequestContext requestContext,
        final QualifiedName tableName
    ) {
        final long start = registry.clock().wallTime();
        // Handler for reading the result set
        final ResultSetExtractor<Integer> handler = rs -> {
            int count = 0;
            while (rs.next()) {
                count = rs.getInt("count");
            }
            return count;
        };
        try {
            final Optional<QualifiedName> sourceTable
                = getSourceTableName(tableName.getDatabaseName(), tableName.getTableName(),
                false);
            return sourceTable.map(
                qualifiedName ->
                    jdbcTemplate.query(SQL.SQL_GET_AUDIT_TABLE_PARTITION_COUNT,
                        new String[]{
                            tableName.getDatabaseName(),
                            tableName.getTableName(),
                            qualifiedName.getDatabaseName(),
                            qualifiedName.getTableName(), },
                        new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR}, handler))
                .orElseGet(
                    () -> jdbcTemplate.query(SQL.SQL_GET_PARTITION_COUNT,
                        new String[]{
                            tableName.getDatabaseName(),
                            tableName.getTableName(), },
                        new int[]{Types.VARCHAR, Types.VARCHAR}, handler));
        } catch (Exception e) {
            throw new ConnectorException("Failed getting the partition count", e);
        } finally {
            this.fastServiceMetric.recordTimer(
                HiveMetrics.TagGetPartitionCount.getMetricName(), registry.clock().wallTime() - start);
        }
    }

    /**
     * Gets the Partitions based on a filter expression for the specified table.
     *
     * @param requestContext    The Metacat request context
     * @param tableName         table name
     * @param partitionsRequest The metadata for what kind of partitions to get from the table
     * @return filtered list of partitions
     */
    @Transactional(readOnly = true)
    public List<PartitionInfo> getPartitions(
        final ConnectorRequestContext requestContext,
        final QualifiedName tableName,
        final PartitionListRequest partitionsRequest
    ) {
        final long start = registry.clock().wallTime();
        try {
            return this.getPartitions(
                tableName.getDatabaseName(),
                tableName.getTableName(),
                partitionsRequest.getPartitionNames(),
                partitionsRequest.getFilter(),
                partitionsRequest.getSort(),
                partitionsRequest.getPageable(),
                partitionsRequest.getIncludePartitionDetails(),
                partitionsRequest.getIncludeAuditOnly()
            ).stream().map(PartitionHolder::getPartitionInfo).collect(Collectors.toList());
        } finally {
            this.fastServiceMetric.recordTimer(
                HiveMetrics.TagGetPartitions.getMetricName(), registry.clock().wallTime() - start);
        }
    }

    /**
     * Gets the partition uris based on a filter expression for the specified table.
     *
     * @param requestContext    The Metacat request context
     * @param tableName         table handle to get partition for
     * @param partitionsRequest The metadata for what kind of partitions to get from the table
     * @return filtered list of partition names
     */
    @Transactional(readOnly = true)
    public List<String> getPartitionUris(final ConnectorRequestContext requestContext,
                                         final QualifiedName tableName,
                                         final PartitionListRequest partitionsRequest) {
        final long start = registry.clock().wallTime();
        final List<String> result;
        final List<String> partitionNames = partitionsRequest.getPartitionNames();
        final Sort sort = partitionsRequest.getSort();
        final Pageable pageable = partitionsRequest.getPageable();

        final String filterExpression = partitionsRequest.getFilter();

        if (filterExpression != null) {
            return filterPartitionsColumn(
                tableName.getDatabaseName(),
                tableName.getTableName(),
                partitionNames,
                PARTITION_URI,
                filterExpression,
                sort,
                pageable,
                partitionsRequest.getIncludeAuditOnly());
        } else {
            final ResultSetExtractor<List<String>> handler = rs -> {
                final List<String> uris = Lists.newArrayList();
                while (rs.next()) {
                    uris.add(rs.getString(PARTITION_URI));
                }
                return uris;
            };
            result = getHandlerResults(tableName.getDatabaseName(), tableName.getTableName(),
                null, partitionNames, SQL.SQL_GET_PARTITIONS_URI, handler, sort, pageable,
                partitionsRequest.getIncludeAuditOnly());
        }
        this.fastServiceMetric.recordTimer(
            HiveMetrics.TagGetPartitionKeys.getMetricName(), registry.clock().wallTime() - start);
        return result;
    }


    /**
     * query partitions using filters from name or uri column.
     */
    private List<String> filterPartitionsColumn(
        final String databaseName,
        final String tableName,
        final List<String> partitionNames,
        final String columnName,
        final String filterExpression,
        final Sort sort,
        final Pageable pageable,
        final boolean forceDisableAudit) {
        final FilterPartition filter = new FilterPartition();
        // batch exists
        final boolean isBatched =
            !Strings.isNullOrEmpty(filterExpression) && filterExpression.contains(FIELD_BATCHID);
        final boolean hasDateCreated =
            !Strings.isNullOrEmpty(filterExpression) && filterExpression.contains(FIELD_DATE_CREATED);

        ResultSetExtractor<List<String>> handler = rs -> {
            final List<String> columns = Lists.newArrayList();
            while (rs.next()) {
                final String name = rs.getString(PARTITION_NAME);
                final String uri = rs.getString(PARTITION_URI);
                final long createdDate = rs.getLong(FIELD_DATE_CREATED);
                Map<String, String> values = null;
                if (hasDateCreated) {
                    values = Maps.newHashMap();
                    values.put(FIELD_DATE_CREATED, createdDate + "");
                }
                if (Strings.isNullOrEmpty(filterExpression)
                    || filter.evaluatePartitionExpression(filterExpression, name, uri, isBatched, values)) {
                    columns.add(rs.getString(columnName));
                }
            }
            return columns;
        };
        return getHandlerResults(databaseName,
            tableName, filterExpression, partitionNames,
            SQL.SQL_GET_PARTITIONS_WITH_KEY_URI, handler, sort, pageable, forceDisableAudit);
    }

    /**
     * Gets the partition names/keys based on a filter expression for the specified table.
     *
     * @param requestContext    The Metacat request context
     * @param tableName         table handle to get partition for
     * @param partitionsRequest The metadata for what kind of partitions to get from the table
     * @return filtered list of partition names
     */
    @Transactional(readOnly = true)
    public List<String> getPartitionKeys(final ConnectorRequestContext requestContext,
                                         final QualifiedName tableName,
                                         final PartitionListRequest partitionsRequest) {
        final long start = registry.clock().wallTime();
        final List<String> result;
        final List<String> partitionNames = partitionsRequest.getPartitionNames();
        final Sort sort = partitionsRequest.getSort();
        final Pageable pageable = partitionsRequest.getPageable();

        final String filterExpression = partitionsRequest.getFilter();
        if (filterExpression != null) {
            return filterPartitionsColumn(
                tableName.getDatabaseName(),
                tableName.getTableName(),
                partitionNames,
                PARTITION_NAME,
                filterExpression,
                sort,
                pageable,
                partitionsRequest.getIncludeAuditOnly());
        } else {
            final ResultSetExtractor<List<String>> handler = rs -> {
                final List<String> names = Lists.newArrayList();
                while (rs.next()) {
                    names.add(rs.getString("name"));
                }
                return names;
            };
            result = getHandlerResults(tableName.getDatabaseName(), tableName.getTableName(),
                null, partitionNames, SQL.SQL_GET_PARTITIONS_WITH_KEY,
                handler, sort, pageable, partitionsRequest.getIncludeAuditOnly());
        }
        this.fastServiceMetric.recordTimer(
            HiveMetrics.TagGetPartitionKeys.getMetricName(), registry.clock().wallTime() - start);
        return result;
    }

    /**
     * getPartitionNames.
     *
     * @param context      request context
     * @param uris         uris
     * @param prefixSearch prefixSearch
     * @return partition names
     */
    @Transactional(readOnly = true)
    public Map<String, List<QualifiedName>> getPartitionNames(
        @Nonnull final ConnectorRequestContext context,
        @Nonnull final List<String> uris,
        final boolean prefixSearch) {
        final long start = registry.clock().wallTime();
        final Map<String, List<QualifiedName>> result = Maps.newHashMap();
        // Create the sql
        final StringBuilder queryBuilder = new StringBuilder(SQL.SQL_GET_PARTITION_NAMES_BY_URI);
        final List<SqlParameterValue> params = Lists.newArrayList();
        if (prefixSearch) {
            queryBuilder.append(" 1=2");
            uris.forEach(uri -> {
                queryBuilder.append(" or location like ?");
                params.add(new SqlParameterValue(Types.VARCHAR, uri + "%"));
            });
        } else {
            queryBuilder.append(" location in (");
            Joiner.on(',').appendTo(queryBuilder, uris.stream().map(uri -> "?").collect(Collectors.toList()));
            queryBuilder.append(")");
            params.addAll(uris.stream()
                .map(uri -> new SqlParameterValue(Types.VARCHAR, uri)).collect(Collectors.toList()));
        }

        final ResultSetExtractor<Map<String, List<QualifiedName>>> handler = rs -> {
            while (rs.next()) {
                final String schemaName = rs.getString("schema_name");
                final String tableName = rs.getString("table_name");
                final String partitionName = rs.getString("partition_name");
                final String uri = rs.getString("location");
                final List<QualifiedName> partitionNames = result.get(uri);
                final QualifiedName qualifiedName =
                    QualifiedName.ofPartition(catalogName, schemaName, tableName, partitionName);
                if (partitionNames == null) {
                    result.put(uri, Lists.newArrayList(qualifiedName));
                } else {
                    partitionNames.add(qualifiedName);
                }
            }
            return result;
        };
        try {
            jdbcTemplate.query(queryBuilder.toString(), params.toArray(), handler);
        } finally {
            this.fastServiceMetric.recordTimer(
                HiveMetrics.TagGetPartitionNames.getMetricName(), registry.clock().wallTime() - start);
        }
        return result;
    }

    @Transactional(readOnly = true)
    protected Map<String, PartitionHolder> getPartitionHoldersByNames(final Table table,
                                                                      final List<String> partitionNames,
                                                                      final boolean forceDisableAudit) {
        //this is internal call to get partitions, always set the forceDisableAudit = true
        return this.getPartitions(
            table.getDbName(),
            table.getTableName(),
            partitionNames,
            null,
            null,
            null,
            false,
            forceDisableAudit
        ).stream().collect(Collectors.toMap(
            p -> p.getPartitionInfo().getName().getPartitionName(),
            p -> p)
        );
    }

    private List<PartitionHolder> getPartitions(
        final String databaseName,
        final String tableName,
        @Nullable final List<String> partitionIds,
        @Nullable final String filterExpression,
        @Nullable final Sort sort,
        @Nullable final Pageable pageable,
        final boolean includePartitionDetails,
        final boolean forceDisableAudit
    ) {
        final FilterPartition filter = new FilterPartition();
        // batch exists
        final boolean isBatched = !Strings.isNullOrEmpty(filterExpression) && filterExpression.contains(FIELD_BATCHID);
        final boolean hasDateCreated =
            !Strings.isNullOrEmpty(filterExpression) && filterExpression.contains(FIELD_DATE_CREATED);
        // Handler for reading the result set
        final ResultSetExtractor<List<PartitionHolder>> handler = rs -> {
            final List<PartitionHolder> result = Lists.newArrayList();
            final QualifiedName tableQName = QualifiedName.ofTable(catalogName, databaseName, tableName);
            int noOfRows = 0;
            while (rs.next()) {
                noOfRows++;
                final String name = rs.getString("name");
                final String uri = rs.getString("uri");
                final long createdDate = rs.getLong(FIELD_DATE_CREATED);
                Map<String, String> values = null;
                if (hasDateCreated) {
                    values = Maps.newHashMap();
                    values.put(FIELD_DATE_CREATED, createdDate + "");
                }
                if (Strings.isNullOrEmpty(filterExpression)
                    || filter.evaluatePartitionExpression(filterExpression, name, uri, isBatched, values)) {
                    final Long id = rs.getLong("id");
                    final Long sdId = rs.getLong("sd_id");
                    final Long serdeId = rs.getLong("serde_id");
                    final String inputFormat = rs.getString("input_format");
                    final String outputFormat = rs.getString("output_format");
                    final String serializationLib = rs.getString("slib");
                    final StorageInfo storageInfo = new StorageInfo();
                    storageInfo.setUri(uri);
                    storageInfo.setInputFormat(inputFormat);
                    storageInfo.setOutputFormat(outputFormat);
                    storageInfo.setSerializationLib(serializationLib);
                    final AuditInfo auditInfo = new AuditInfo();
                    auditInfo.setCreatedDate(Date.from(Instant.ofEpochSecond(createdDate)));
                    auditInfo.setLastModifiedDate(Date.from(Instant.ofEpochSecond(createdDate)));

                    result.add(new PartitionHolder(id, sdId, serdeId,
                        PartitionInfo.builder().name(QualifiedName.ofPartition(catalogName,
                            databaseName, tableName, name)).auditInfo(auditInfo).serde(storageInfo).build()));
                }

                // Fail if the number of partitions exceeds the threshold limit.
                if (result.size() > config.getMaxPartitionsThreshold()) {
                    registry.counter(registry.createId(HiveMetrics.CounterHiveGetPartitionsExceedThresholdFailure
                            .getMetricName()).withTags(tableQName.parts())).increment();
                    final String message =
                        String.format("Number of partitions queried for table %s exceeded the threshold %d",
                            tableQName, config.getMaxPartitionsThreshold());
                    log.warn(message);
                    throw new IllegalArgumentException(message);
                }
            }
            registry.gauge(registry.createId(HiveMetrics.GaugePreExpressionFilterGetPartitionsCount
                .getMetricName()).withTags(tableQName.parts())).set(noOfRows);
            return result;
        };

        final List<PartitionHolder> partitions = this.getHandlerResults(
            databaseName,
            tableName,
            filterExpression,
            partitionIds,
            SQL.SQL_GET_PARTITIONS,
            handler,
            sort,
            pageable,
            forceDisableAudit
        );

        if (includePartitionDetails && !partitions.isEmpty()) {
            final List<Long> partIds = Lists.newArrayListWithCapacity(partitions.size());
            final List<Long> sdIds = Lists.newArrayListWithCapacity(partitions.size());
            final List<Long> serdeIds = Lists.newArrayListWithCapacity(partitions.size());
            for (PartitionHolder partitionHolder : partitions) {
                partIds.add(partitionHolder.getId());
                sdIds.add(partitionHolder.getSdId());
                serdeIds.add(partitionHolder.getSerdeId());
            }
            final List<ListenableFuture<Void>> futures = Lists.newArrayList();
            final Map<Long, Map<String, String>> partitionParams = Maps.newHashMap();
            futures.add(threadServiceManager.getExecutor().submit(() ->
                populateParameters(partIds, SQL.SQL_GET_PARTITION_PARAMS,
                    "part_id", partitionParams)));

            final Map<Long, Map<String, String>> sdParams = Maps.newHashMap();
            if (!sdIds.isEmpty()) {
                futures.add(threadServiceManager.getExecutor().submit(() ->
                    populateParameters(sdIds, SQL.SQL_GET_SD_PARAMS,
                        "sd_id", sdParams)));
            }
            final Map<Long, Map<String, String>> serdeParams = Maps.newHashMap();
            if (!serdeIds.isEmpty()) {
                futures.add(threadServiceManager.getExecutor().submit(() ->
                    populateParameters(serdeIds, SQL.SQL_GET_SERDE_PARAMS,
                        "serde_id", serdeParams)));
            }
            ListenableFuture<List<Void>> future = null;
            try {
                future = Futures.allAsList(futures);
                final int getPartitionsDetailsTimeout = Integer.parseInt(configuration
                    .getOrDefault(HiveConfigConstants.GET_PARTITION_DETAILS_TIMEOUT, "120"));
                future.get(getPartitionsDetailsTimeout, TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                try {
                    if (future != null) {
                        future.cancel(true);
                    }
                } catch (Exception ignored) {
                    log.warn("Failed cancelling the task that gets the partition details.");
                }
                Throwables.propagate(e);
            }

            for (PartitionHolder partitionHolder : partitions) {
                partitionHolder.getPartitionInfo().setMetadata(partitionParams.get(partitionHolder.getId()));
                partitionHolder.getPartitionInfo().getSerde()
                    .setParameters(sdParams.get(partitionHolder.getSdId()));
                partitionHolder.getPartitionInfo().getSerde()
                    .setSerdeInfoParameters(serdeParams.get(partitionHolder.getSerdeId()));

            }
        }
        return partitions;
    }

    private <T> List<T> getHandlerResults(
        final String databaseName,
        final String tableName,
        @Nullable final String filterExpression,
        @Nullable final List<String> partitionIds,
        final String sql,
        final ResultSetExtractor<List<T>> resultSetExtractor,
        @Nullable final Sort sort,
        @Nullable final Pageable pageable,
        final boolean forceDisableAudit
    ) {
        List<T> partitions;
        final QualifiedName tableQName = QualifiedName.ofTable(catalogName, databaseName, tableName);
        try {
            if (!Strings.isNullOrEmpty(filterExpression)) {
                final PartitionFilterGenerator generator =
                    new PartitionFilterGenerator(getPartitionKeys(databaseName, tableName, forceDisableAudit));
                String filterSql = (String) new PartitionParser(new StringReader(filterExpression)).filter()
                    .jjtAccept(generator, null);
                if (generator.isOptimized()) {
                    filterSql = generator.getOptimizedSql();
                }
                if (filterSql != null && !filterSql.isEmpty()) {
                    filterSql = " and (" + filterSql + ")";
                }
                partitions = getHandlerResults(databaseName, tableName, filterExpression, partitionIds,
                    sql, resultSetExtractor,
                    generator.joinSql(), filterSql,
                    generator.getParams(), sort, pageable, forceDisableAudit);
            } else {
                partitions = getHandlerResults(databaseName, tableName, null, partitionIds,
                    sql, resultSetExtractor,
                    null, null,
                    null, sort, pageable, forceDisableAudit);
            }
        } catch (Exception e) {
            log.warn("Experiment: Get partitions for for table {} filter {}"
                    + " failed with error {}", tableQName.toString(), filterExpression,
                e.getMessage());
            registry.counter(registry
                .createId(HiveMetrics.CounterHiveExperimentGetTablePartitionsFailure.getMetricName())
                .withTags(tableQName.parts())).increment();
            partitions = getHandlerResults(databaseName, tableName,
                filterExpression, partitionIds, sql, resultSetExtractor, null,
                prepareFilterSql(filterExpression), Lists.newArrayList(), sort, pageable, forceDisableAudit);
        }
        return partitions;
    }

    private List<FieldSchema> getPartitionKeys(final String databaseName,
                                               final String tableName,
                                               final boolean forceDisableAudit) {
        final List<FieldSchema> result = Lists.newArrayList();
        final ResultSetExtractor<List<FieldSchema>> handler = rs -> {
            while (rs.next()) {
                final String name = rs.getString("pkey_name");
                final String type = rs.getString("pkey_type");
                result.add(new FieldSchema(name, type, null));
            }
            return result;
        };
        final Optional<QualifiedName> sourceTable = getSourceTableName(databaseName, tableName, forceDisableAudit);

        return sourceTable.map(qualifiedName -> jdbcTemplate
            .query(SQL.SQL_GET_AUDIT_TABLE_PARTITION_KEYS,
                new Object[]{databaseName, tableName, qualifiedName.getDatabaseName(), qualifiedName.getTableName()},
                new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR},
                handler))
            .orElseGet(() -> jdbcTemplate
                .query(SQL.SQL_GET_PARTITION_KEYS,
                    new Object[]{databaseName, tableName},
                    new int[]{Types.VARCHAR, Types.VARCHAR},
                    handler));
    }

    private String getDateCreatedSqlCriteria(final String filterExpression) {
        final StringBuilder result = new StringBuilder();
        Collection<String> values = Lists.newArrayList();
        if (!Strings.isNullOrEmpty(filterExpression)) {
            try {
                values = (Collection<String>) new PartitionParser(
                    new StringReader(filterExpression)).filter().jjtAccept(new PartitionParamParserEval(),
                    null
                );
            } catch (Throwable ignored) {
                //
            }
        }
        for (String value : values) {
            if (result.length() != 0) {
                result.append(" and ");
            }
            result.append(value.replace("dateCreated", "p.CREATE_TIME"));
        }
        return result.toString();
    }

    private Void populateParameters(final List<Long> ids,
                                    final String sql,
                                    final String idName,
                                    final Map<Long, Map<String, String>> params) {
        if (ids.size() > 5000) {
            final List<List<Long>> subFilterPartitionNamesList = Lists.partition(ids, 5000);
            subFilterPartitionNamesList.forEach(subPartitions ->
                params.putAll(this.getParameters(subPartitions, sql, idName)));
        } else {
            params.putAll(this.getParameters(ids, sql, idName));
        }
        return null;
    }

    private Map<Long, Map<String, String>> getParameters(final List<Long> ids, final String sql, final String idName) {
        // Create the sql
        final StringBuilder queryBuilder = new StringBuilder(sql);
        if (!ids.isEmpty()) {
            queryBuilder.append(" and ").append(idName)
                .append(" in ('").append(Joiner.on("','").skipNulls().join(ids)).append("')");
        }
        final ResultSetExtractor<Map<Long, Map<String, String>>> handler = rs -> {
            final Map<Long, Map<String, String>> result = Maps.newHashMap();
            while (rs.next()) {
                final Long id = rs.getLong(idName);
                final String key = rs.getString("param_key");
                final String value = rs.getString("param_value");
                final Map<String, String> parameters = result.computeIfAbsent(id, k -> Maps.newHashMap());
                parameters.put(key, value);
            }
            return result;
        };
        return jdbcTemplate.query(queryBuilder.toString(), handler);
    }

    private Collection<String> getSinglePartitionExprs(@Nullable final String filterExpression) {
        Collection<String> result = Lists.newArrayList();
        if (!Strings.isNullOrEmpty(filterExpression)) {
            try {
                result = (Collection<String>) new PartitionParser(
                    new StringReader(filterExpression)).filter().jjtAccept(new PartitionKeyParserEval(),
                    null
                );
            } catch (Throwable ignored) {
                //
            }
        }
        if (result != null) {
            result = result.stream().filter(s -> !(s.startsWith("batchid=") || s.startsWith("dateCreated="))).collect(
                Collectors.toList());
        }
        return result;
    }

    private String prepareFilterSql(@Nullable final String filterExpression) {
        final StringBuilder result = new StringBuilder();
        // Support for dateCreated
        final boolean hasDateCreated =
            !Strings.isNullOrEmpty(filterExpression) && filterExpression.contains(FIELD_DATE_CREATED);
        String dateCreatedSqlCriteria = null;
        if (hasDateCreated) {
            dateCreatedSqlCriteria = getDateCreatedSqlCriteria(filterExpression);
        }
        final Collection<String> singlePartitionExprs = getSinglePartitionExprs(filterExpression);
        for (String singlePartitionExpr : singlePartitionExprs) {
            result.append(" and p.PART_NAME like '%").append(singlePartitionExpr).append("%'");
        }
        if (!Strings.isNullOrEmpty(dateCreatedSqlCriteria)) {
            result.append(" and ").append(dateCreatedSqlCriteria);
        }
        return result.toString();
    }

    private <T> List<T> getHandlerResults(
        final String databaseName,
        final String tableName,
        @Nullable final String filterExpression,
        @Nullable final List<String> partitionIds,
        final String sql,
        final ResultSetExtractor resultSetExtractor,
        @Nullable final String joinSql,
        @Nullable final String filterSql,
        @Nullable final List<Object> filterParams,
        @Nullable final Sort sort,
        @Nullable final Pageable pageable,
        final boolean forceDisableAudit
    ) {
        //
        // Limiting the in clause to 5000 part names because the sql query with the IN clause for part_name(767 bytes)
        // will hit the max sql query length(max_allowed_packet for our RDS) if we use more than 5400 or so
        //
        List<T> partitions = Lists.newArrayList();
        if (partitionIds != null && partitionIds.size() > 5000) {
            final List<List<String>> subFilterPartitionNamesList = Lists.partition(partitionIds, 5000);
            final List<T> finalPartitions = partitions;
            subFilterPartitionNamesList.forEach(
                subPartitionIds -> finalPartitions.addAll(
                    this.getSubHandlerResultsFromQuery(
                        databaseName,
                        tableName,
                        filterExpression,
                        subPartitionIds,
                        sql,
                        resultSetExtractor,
                        joinSql,
                        filterSql,
                        filterParams,
                        sort,
                        pageable,
                        forceDisableAudit
                    )
                )
            );
        } else {
            partitions = this.getSubHandlerResultsFromQuery(
                databaseName,
                tableName,
                filterExpression,
                partitionIds,
                sql,
                resultSetExtractor,
                joinSql,
                filterSql,
                filterParams,
                sort,
                pageable,
                forceDisableAudit
            );
        }
        return partitions;
    }

    private <T> List<T> getSubHandlerResultsFromQuery(
        final String databaseName,
        final String tableName,
        @Nullable final String filterExpression,
        @Nullable final List<String> partitionIds,
        final String sql,
        final ResultSetExtractor resultSetExtractor,
        @Nullable final String joinSql,
        @Nullable final String filterSql,
        @Nullable final List<Object> filterParams,
        @Nullable final Sort sort,
        @Nullable final Pageable pageable,
        final boolean forceDisableAudit
    ) {
        if (getSourceTableName(databaseName, tableName, forceDisableAudit).isPresent()) {
            return this.getSubHandlerAuditTableResults(
                databaseName,
                tableName,
                filterExpression,
                partitionIds,
                sql,
                resultSetExtractor,
                joinSql,
                filterSql,
                filterParams,
                sort,
                pageable,
                forceDisableAudit
            );
        } else {
            return this.getSubHandlerResults(
                databaseName,
                tableName,
                filterExpression,
                partitionIds,
                sql,
                resultSetExtractor,
                joinSql,
                filterSql,
                filterParams,
                sort,
                pageable
            );
        }
    }

    private <T> List<T> getSubHandlerResults(
        final String databaseName,
        final String tableName,
        @Nullable final String filterExpression,
        @Nullable final List<String> partitionIds,
        final String sql,
        final ResultSetExtractor resultSetExtractor,
        @Nullable final String joinSql,
        @Nullable final String filterSql,
        @Nullable final List<Object> filterParams,
        @Nullable final Sort sort,
        @Nullable final Pageable pageable
    ) {
        // Create the sql
        final StringBuilder queryBuilder = getBasicPartitionQuery(partitionIds, sql, joinSql, filterSql);
        addSortPageableFilter(queryBuilder, filterExpression, sort, pageable);

        List<T> partitions;
        final ImmutableList.Builder<Object> paramsBuilder = ImmutableList.builder().add(databaseName, tableName);
        if (partitionIds != null && !partitionIds.isEmpty()) {
            paramsBuilder.addAll(partitionIds);
        }
        if (filterSql != null && filterParams != null) {
            paramsBuilder.addAll(filterParams);
        }
        final List<Object> params = paramsBuilder.build();
        final Object[] oParams = new Object[params.size()];
        partitions = (List) jdbcTemplate.query(
            queryBuilder.toString(), params.toArray(oParams), resultSetExtractor);

        if (pageable != null && pageable.isPageable() && !Strings.isNullOrEmpty(filterExpression)) {
            partitions = processPageable(partitions, pageable);
        }
        return partitions;
    }

    /**
     * Check if an audit table, i.e. the database is audit and the table name matches WAP table pattern
     *
     * @param databaseName database
     * @param tableName    table name
     * @return true or false
     */
    private Optional<QualifiedName> getSourceTableName(final String databaseName,
                                                       final String tableName,
                                                       final boolean forceDisableAudit) {
        Optional<QualifiedName> sourceTable = Optional.empty();
        final boolean isAuditProcessingEnabled = Boolean.valueOf(configuration
            .getOrDefault(HiveConfigConstants.ENABLE_AUDIT_PROCESSING, "true"));
        if (!forceDisableAudit && isAuditProcessingEnabled && databaseName.equals(AUDIT_DB)) {
            final Matcher matcher = AUDIT_TABLENAME_PATTERN.matcher(tableName);
            if (matcher.matches()) {
                final String sourceDatabaseName = matcher.group("db");
                final String sourceTableName = matcher.group("table");
                sourceTable = Optional.of(QualifiedName.ofTable(this.catalogName, sourceDatabaseName, sourceTableName));
            }
        }
        return sourceTable;
    }

    /**
     * Process audit table partition related query.
     *
     * @param databaseName       database name
     * @param tableName          table name
     * @param filterExpression   filter
     * @param partitionIds       partition ids
     * @param sql                query sql
     * @param resultSetExtractor result extractor
     * @param joinSql            join sql
     * @param filterSql          filter sql
     * @param filterParams       filter parameters
     * @param sort               sort object
     * @param pageable           pageable object
     * @param <T>                query object
     * @return query results
     */
    private <T> List<T> getSubHandlerAuditTableResults(
        final String databaseName,
        final String tableName,
        @Nullable final String filterExpression,
        @Nullable final List<String> partitionIds,
        final String sql,
        final ResultSetExtractor resultSetExtractor,
        @Nullable final String joinSql,
        @Nullable final String filterSql,
        @Nullable final List<Object> filterParams,
        @Nullable final Sort sort,
        @Nullable final Pageable pageable,
        final boolean forceDisableAudit
    ) {
        final Optional<QualifiedName> sourceTableName = getSourceTableName(databaseName, tableName, forceDisableAudit);
        List<T> partitions = Lists.newArrayList();
        if (sourceTableName.isPresent()) {
            final StringBuilder auditTableQueryBuilder = getBasicPartitionQuery(partitionIds, sql, joinSql, filterSql);
            final StringBuilder sourceTableQueryBuilder = getBasicPartitionQuery(partitionIds, sql, joinSql, filterSql)
                .append(SQL.SQL_NOT_IN_AUTDI_TABLE_PARTITIONS);

            //union the two queries, using ALL for optimization since the above sql already filtered out the overlap
            //partitions from the source table
            auditTableQueryBuilder.append(" UNION ALL ").append(sourceTableQueryBuilder);

            addSortPageableFilter(auditTableQueryBuilder, filterExpression, sort, pageable);

            // Params
            final ImmutableList.Builder<Object> paramsBuilder = ImmutableList.builder().add(databaseName, tableName);
            if (partitionIds != null && !partitionIds.isEmpty()) {
                paramsBuilder.addAll(partitionIds);
            }
            if (filterSql != null && filterParams != null) {
                paramsBuilder.addAll(filterParams);
            }
            paramsBuilder.add(sourceTableName.get().getDatabaseName(), sourceTableName.get().getTableName());
            if (partitionIds != null && !partitionIds.isEmpty()) {
                paramsBuilder.addAll(partitionIds);
            }
            if (filterSql != null && filterParams != null) {
                paramsBuilder.addAll(filterParams);
            }
            paramsBuilder.add(databaseName, tableName);
            final List<Object> params = paramsBuilder.build();
            final Object[] oParams = new Object[params.size()];
            partitions = (List) jdbcTemplate.query(
                auditTableQueryBuilder.toString(), params.toArray(oParams), resultSetExtractor);
            if (pageable != null && pageable.isPageable() && !Strings.isNullOrEmpty(filterExpression)) {
                partitions = processPageable(partitions, pageable);
            }
        }
        return partitions;
    }

    private StringBuilder getBasicPartitionQuery(
        @Nullable final List<String> partitionIds,
        final String sql,
        @Nullable final String joinSql,
        @Nullable final String filterSql
    ) {
        final StringBuilder tableQueryBuilder = new StringBuilder(sql);
        if (joinSql != null) {
            tableQueryBuilder.append(joinSql);
        }
        tableQueryBuilder.append(" where d.NAME = ? and t.TBL_NAME = ?");
        if (filterSql != null) {
            tableQueryBuilder.append(filterSql);
        }
        if (partitionIds != null && !partitionIds.isEmpty()) {
            final List<String> paramVariables = partitionIds.stream().map(s -> "?").collect(Collectors.toList());
            tableQueryBuilder.append(" and p.PART_NAME in (")
                .append(Joiner.on(",").skipNulls().join(paramVariables)).append(")");
        }
        return tableQueryBuilder;
    }

    //adding the sort and limit to sql query
    private void addSortPageableFilter(
        final StringBuilder queryBuilder,
        @Nullable final String filterExpression,
        @Nullable final Sort sort,
        @Nullable final Pageable pageable
    ) {
        if (sort != null && sort.hasSort()) {
            queryBuilder.append(" order by ").append(sort.getSortBy()).append(" ").append(sort.getOrder().name());
        }
        if (pageable != null && pageable.isPageable() && Strings.isNullOrEmpty(filterExpression)) {
            if (sort == null || !sort.hasSort()) {
                queryBuilder.append(" order by id");
                //this must be id, which is used by AuditTable and regular table pagination
            }
            queryBuilder.append(" limit ").append(pageable.getOffset()).append(',').append(pageable.getLimit());
        }
    }

    private <T> List<T> processPageable(final List<T> partitions,
                                        final Pageable pageable) {
        int limit = pageable.getOffset() + pageable.getLimit();
        if (partitions.size() < limit) {
            limit = partitions.size();
        }
        if (pageable.getOffset() > limit) {
            return Lists.newArrayList();
        } else {
            return partitions.subList(pageable.getOffset(), limit);
        }
    }

    @VisibleForTesting
    private static class SQL {
        static final String SQL_GET_PARTITIONS_WITH_KEY_URI =
            //Add p.part_id as id to allow pagination using 'order by id'
            "select p.part_id as id, p.PART_NAME as name, p.CREATE_TIME as dateCreated, sds.location uri"
                + " from PARTITIONS as p join TBLS as t on t.TBL_ID = p.TBL_ID "
                + "join DBS as d on t.DB_ID = d.DB_ID join SDS as sds on p.SD_ID = sds.SD_ID";
        static final String SQL_GET_PARTITIONS_URI =
            "select p.part_id as id, sds.location uri"
                + " from PARTITIONS as p join TBLS as t on t.TBL_ID = p.TBL_ID "
                + "join DBS as d on t.DB_ID = d.DB_ID join SDS as sds on p.SD_ID = sds.SD_ID";

        static final String SQL_GET_PARTITIONS_WITH_KEY =
            "select p.part_id as id, p.PART_NAME as name from PARTITIONS as p"
                + " join TBLS as t on t.TBL_ID = p.TBL_ID join DBS as d on t.DB_ID = d.DB_ID";
        static final String SQL_GET_PARTITIONS =
            "select p.part_id as id, p.PART_NAME as name, p.CREATE_TIME as dateCreated,"
                + " sds.location uri, sds.input_format, sds.output_format,"
                + " sds.sd_id, s.serde_id, s.slib from PARTITIONS as p"
                + " join TBLS as t on t.TBL_ID = p.TBL_ID join DBS as d"
                + " on t.DB_ID = d.DB_ID join SDS as sds on p.SD_ID = sds.SD_ID"
                + " join SERDES s on sds.SERDE_ID=s.SERDE_ID";
        static final String SQL_GET_PARTITION_NAMES_BY_URI =
            "select p.part_name partition_name,t.tbl_name table_name,d.name schema_name,"
                + " sds.location from PARTITIONS as p join TBLS as t on t.TBL_ID = p.TBL_ID"
                + " join DBS as d on t.DB_ID = d.DB_ID join SDS as sds on p.SD_ID = sds.SD_ID where";
        static final String SQL_GET_PARTITION_PARAMS =
            "select part_id, param_key, param_value from PARTITION_PARAMS where 1=1";
        static final String SQL_GET_SD_PARAMS =
            "select sd_id, param_key, param_value from SD_PARAMS where 1=1";
        static final String SQL_GET_SERDE_PARAMS =
            "select serde_id, param_key, param_value from SERDE_PARAMS where 1=1";
        static final String SQL_GET_PARTITION_KEYS =
            "select pkey_name, pkey_type from PARTITION_KEYS as p "
                + "join TBLS as t on t.TBL_ID = p.TBL_ID join DBS as d"
                + " on t.DB_ID = d.DB_ID where d.name=? and t.tbl_name=? order by integer_idx";
        static final String SQL_GET_PARTITION_COUNT =
            "select count(*) count from PARTITIONS as p"
                + " join TBLS as t on t.TBL_ID = p.TBL_ID join DBS as d on t.DB_ID = d.DB_ID"
                + " where d.NAME = ? and t.TBL_NAME = ?";

        //audit table, takes precedence in case there are parititons overlap with the source
        static final String SQL_GET_AUDIT_TABLE_PARTITION_COUNT =
            "select count(distinct p1.part_name) count from PARTITIONS as p1 "
                + "join TBLS as t1 on t1.TBL_ID = p1.TBL_ID join DBS as d1 on t1.DB_ID = d1.DB_ID "
                + "where ( d1.NAME = ?  and t1.TBL_NAME = ? ) "
                + "or ( d1.NAME = ? and t1.TBL_NAME = ?)";

        // using nest order https://stackoverflow.com/questions/6965333/mysql-union-distinct
        static final String SQL_GET_AUDIT_TABLE_PARTITION_KEYS =
            "select pkey_name, pkey_type from ("
                + "select pkey_name, pkey_type, integer_idx from PARTITION_KEYS as p1 "
                + "join TBLS as t1 on t1.TBL_ID = p1.TBL_ID join DBS as d1 "
                + "on t1.DB_ID = d1.DB_ID where d1.NAME = ? and t1.TBL_NAME = ? order by integer_idx "
                + ") as tmp "
                + "UNION "
                + "select pkey_name, pkey_type from PARTITION_KEYS as p2 "
                + "join TBLS as t2 on t2.TBL_ID = p2.TBL_ID join DBS as d2 "
                + "on t2.DB_ID = d2.DB_ID where d2.NAME = ? and t2.TBL_NAME = ?";

        //select the partitions not in audit table
        static final String SQL_NOT_IN_AUTDI_TABLE_PARTITIONS =
            " and p.PART_NAME not in ("
                + " select p1.PART_NAME from PARTITIONS as p1"
                + " join TBLS as t1 on t1.TBL_ID = p1.TBL_ID join DBS as d1"
                + " on t1.DB_ID = d1.DB_ID where d1.NAME = ? and t1.TBL_NAME = ? )";  //audit table
    }
}
