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
package com.netflix.metacat.connector.hive;

import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext;
import com.netflix.metacat.common.server.connectors.exception.ConnectorException;
import com.netflix.metacat.common.server.connectors.model.AuditInfo;
import com.netflix.metacat.common.server.connectors.model.PartitionInfo;
import com.netflix.metacat.common.server.connectors.model.PartitionListRequest;
import com.netflix.metacat.common.server.connectors.model.StorageInfo;
import com.netflix.metacat.common.server.connectors.model.TableInfo;
import com.netflix.metacat.common.server.partition.parser.PartitionParser;
import com.netflix.metacat.common.server.partition.util.FilterPartition;
import com.netflix.metacat.common.server.partition.visitor.PartitionKeyParserEval;
import com.netflix.metacat.common.server.partition.visitor.PartitionParamParserEval;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.util.JdbcUtil;
import com.netflix.metacat.common.server.util.ThreadServiceManager;
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter;
import com.netflix.metacat.connector.hive.monitoring.HiveMetrics;
import com.netflix.metacat.connector.hive.util.PartitionDetail;
import com.netflix.metacat.connector.hive.util.PartitionFilterGenerator;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import java.io.StringReader;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * HiveConnectorFastPartitionService.
 *
 * @author zhenl
 * @since 1.0.0
 */
@Slf4j
@Transactional(readOnly = true)
public class HiveConnectorFastPartitionService extends HiveConnectorPartitionService {
    private static final String FIELD_DATE_CREATED = "dateCreated";
    private static final String FIELD_BATCHID = "batchid";
    private static final String SQL_GET_PARTITIONS_WITH_KEY_URI =
        "select p.PART_NAME as name, p.CREATE_TIME as dateCreated, sds.location uri"
            + " from PARTITIONS as p join TBLS as t on t.TBL_ID = p.TBL_ID "
            + "join DBS as d on t.DB_ID = d.DB_ID join SDS as sds on p.SD_ID = sds.SD_ID";
    private static final String SQL_GET_PARTITIONS_WITH_KEY =
        "select p.PART_NAME as name from PARTITIONS as p"
            + " join TBLS as t on t.TBL_ID = p.TBL_ID join DBS as d on t.DB_ID = d.DB_ID";
    private static final String SQL_GET_PARTITIONS =
        "select p.part_id as id, p.PART_NAME as name, p.CREATE_TIME as dateCreated,"
            + " sds.location uri, sds.input_format, sds.output_format,"
            + " sds.sd_id, s.serde_id, s.slib from PARTITIONS as p"
            + " join TBLS as t on t.TBL_ID = p.TBL_ID join DBS as d"
            + " on t.DB_ID = d.DB_ID join SDS as sds on p.SD_ID = sds.SD_ID"
            + " join SERDES s on sds.SERDE_ID=s.SERDE_ID";
    private static final String SQL_GET_PARTITION_NAMES_BY_URI =
        "select p.part_name partition_name,t.tbl_name table_name,d.name schema_name,"
            + " sds.location from PARTITIONS as p join TBLS as t on t.TBL_ID = p.TBL_ID"
            + " join DBS as d on t.DB_ID = d.DB_ID join SDS as sds on p.SD_ID = sds.SD_ID where";
    private static final String SQL_GET_PARTITION_PARAMS =
        "select part_id, param_key, param_value from PARTITION_PARAMS where 1=1";
    private static final String SQL_GET_SD_PARAMS =
        "select sd_id, param_key, param_value from SD_PARAMS where 1=1";
    private static final String SQL_GET_SERDE_PARAMS =
        "select serde_id, param_key, param_value from SERDE_PARAMS where 1=1";
    private static final String SQL_GET_PARTITION_KEYS =
        "select pkey_name, pkey_type from PARTITION_KEYS as p "
            + "join TBLS as t on t.TBL_ID = p.TBL_ID join DBS as d"
            + " on t.DB_ID = d.DB_ID where d.name=? and t.tbl_name=? order by integer_idx";

    private static final String SQL_GET_PARTITION_COUNT =
        "select count(*) count from PARTITIONS as p"
            + " join TBLS as t on t.TBL_ID = p.TBL_ID join DBS as d on t.DB_ID = d.DB_ID"
            + " join SDS as sds on p.SD_ID = sds.SD_ID where d.NAME = ? and t.TBL_NAME = ?";

    private final ThreadServiceManager threadServiceManager;
    private final Registry registry;
    private final Id requestTimerId;
    private final JdbcUtil jdbcUtil;

    /**
     * Constructor.
     *
     * @param catalogName            catalogname
     * @param metacatHiveClient      hive client
     * @param hiveMetacatConverters  hive converter
     * @param connectorContext server context
     * @param threadServiceManager   thread service manager
     * @param dataSource             data source
     */
    public HiveConnectorFastPartitionService(
        final String catalogName,
        final IMetacatHiveClient metacatHiveClient,
        final HiveConnectorInfoConverter hiveMetacatConverters,
        final ConnectorContext connectorContext,
        final ThreadServiceManager threadServiceManager,
        final DataSource dataSource
    ) {
        super(catalogName, metacatHiveClient, hiveMetacatConverters);
        this.threadServiceManager = threadServiceManager;
        this.registry = connectorContext.getRegistry();
        this.requestTimerId = registry.createId(HiveMetrics.TimerFastHiveRequest.name());
        this.jdbcUtil = new JdbcUtil(dataSource);
    }

    /**
     * Number of partitions for the given table.
     *
     * @param tableName tableName
     * @return Number of partitions
     */
    @Override
    public int getPartitionCount(
        final ConnectorRequestContext requestContext,
        final QualifiedName tableName
    ) {
        final long start = registry.clock().monotonicTime();
        final Map<String, String> tags = new HashMap<String, String>();
        tags.put("request", HiveMetrics.getPartitionCount.name());
        final Integer result;
        // Handler for reading the result set
        final ResultSetExtractor<Integer> handler = rs -> {
            int count = 0;
            while (rs.next()) {
                count = rs.getInt("count");
            }
            return count;
        };
        try {
            result = jdbcUtil.getJdbcTemplate().query(SQL_GET_PARTITION_COUNT, handler,
                tableName.getDatabaseName(), tableName.getTableName());
        } catch (DataAccessException e) {
            throw new ConnectorException("getPartitionCount", e);
        } finally {
            final long duration = registry.clock().monotonicTime() - start;
            log.debug("### Time taken to complete getPartitionCount is {} ms", duration);
            this.registry.timer(requestTimerId.withTags(tags)).record(duration, TimeUnit.MILLISECONDS);
        }
        return result;
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<PartitionInfo> getPartitions(
        final ConnectorRequestContext requestContext,
        final QualifiedName tableName,
        final PartitionListRequest partitionsRequest
    ) {
        final long start = registry.clock().monotonicTime();
        final Map<String, String> tags = new HashMap<String, String>();
        tags.put("request", HiveMetrics.getPartitions.name());
        try {
            return getpartitions(tableName.getDatabaseName(), tableName.getTableName(),
                partitionsRequest.getPartitionNames(),
                partitionsRequest.getFilter(),
                partitionsRequest.getSort(),
                partitionsRequest.getPageable(),
                partitionsRequest.getIncludePartitionDetails());
        } finally {
            final long duration = registry.clock().monotonicTime() - start;
            log.debug("### Time taken to complete getPartitions is {} ms", duration);
            this.registry.timer(requestTimerId.withTags(tags)).record(duration, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<String> getPartitionKeys(final ConnectorRequestContext requestContext,
                                         final QualifiedName tableName,
                                         final PartitionListRequest partitionsRequest) {
        final long start = registry.clock().monotonicTime();
        final Map<String, String> tags = new HashMap<String, String>();
        tags.put("request", "getPartitionKeys");
        final List<String> result;
        final List<String> partitionNames = partitionsRequest.getPartitionNames();
        final Sort sort = partitionsRequest.getSort();
        final Pageable pageable = partitionsRequest.getPageable();

        final String filterExpression = partitionsRequest.getFilter();
        if (filterExpression != null) {
            final FilterPartition filter = new FilterPartition();
            // batch exists
            final boolean isBatched =
                !Strings.isNullOrEmpty(filterExpression) && filterExpression.contains(FIELD_BATCHID);
            final boolean hasDateCreated =
                !Strings.isNullOrEmpty(filterExpression) && filterExpression.contains(FIELD_DATE_CREATED);

            ResultSetExtractor<List<String>> handler = rs -> {
                final List<String> names = Lists.newArrayList();
                while (rs.next()) {
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
                        names.add(name);
                    }
                }
                return names;
            };
            result = getHandlerResults(tableName.getDatabaseName(),
                tableName.getTableName(), filterExpression, partitionNames,
                SQL_GET_PARTITIONS_WITH_KEY_URI, handler, sort, pageable);
        } else {
            final ResultSetExtractor<List<String>> handler = rs -> {
                final List<String> names = Lists.newArrayList();
                while (rs.next()) {
                    names.add(rs.getString("name"));
                }
                return names;
            };
            result = getHandlerResults(tableName.getDatabaseName(), tableName.getTableName(),
                null, partitionNames, SQL_GET_PARTITIONS_WITH_KEY, handler, sort, pageable);
        }
        final long duration = registry.clock().monotonicTime() - start;
        log.debug("### Time taken to complete getPartitionKeys is {} ms", duration);
        this.registry.timer(requestTimerId.withTags(tags)).record(duration, TimeUnit.MILLISECONDS);

        return result;
    }

    /**
     * getPartitionNames.
     *
     * @param uris         uris
     * @param prefixSearch prefixSearch
     * @return partition names
     */
    @Override
    public Map<String, List<QualifiedName>> getPartitionNames(
        @Nonnull final ConnectorRequestContext context,
        @Nonnull final List<String> uris,
        final boolean prefixSearch) {
        final long start = registry.clock().monotonicTime();
        final Map<String, String> tags = new HashMap<String, String>();
        tags.put("request", HiveMetrics.getPartitionNames.name());

        final Map<String, List<QualifiedName>> result = Maps.newHashMap();
        // Create the sql
        final StringBuilder queryBuilder = new StringBuilder(SQL_GET_PARTITION_NAMES_BY_URI);
        final List<String> params = Lists.newArrayList();
        if (prefixSearch) {
            queryBuilder.append(" 1=2");
            uris.forEach(uri -> {
                queryBuilder.append(" or location like ?");
                params.add(uri + "%");
            });
        } else {
            queryBuilder.append(" location in (");
            Joiner.on(',').appendTo(queryBuilder, uris.stream().map(uri -> "?").collect(Collectors.toList()));
            queryBuilder.append(")");
            params.addAll(uris);
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
            jdbcUtil.getJdbcTemplate().query(queryBuilder.toString(), params.toArray(), handler);
        } catch (DataAccessException e) {
            Throwables.propagate(e);
        } finally {
            final long duration = registry.clock().monotonicTime() - start;
            log.debug("### Time taken to complete getPartitionNames is {} ms", duration);
            this.registry.timer(requestTimerId.withTags(tags)).record(duration, TimeUnit.MILLISECONDS);
        }
        return result;
    }

    private List<PartitionInfo> getpartitions(final String databaseName,
                                              final String tableName,
                                              @Nullable final List<String> partitionIds,
                                              @Nullable final String filterExpression,
                                              @Nullable final Sort sort,
                                              @Nullable final Pageable pageable,
                                              final boolean includePartitionDetails) {
        final FilterPartition filter = new FilterPartition();
        // batch exists
        final boolean isBatched = !Strings.isNullOrEmpty(filterExpression) && filterExpression.contains(FIELD_BATCHID);
        final boolean hasDateCreated =
            !Strings.isNullOrEmpty(filterExpression) && filterExpression.contains(FIELD_DATE_CREATED);
        // Handler for reading the result set
        final ResultSetExtractor<List<PartitionDetail>> handler = rs -> {
            final List<PartitionDetail> result = Lists.newArrayList();
            while (rs.next()) {
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

                    result.add(new PartitionDetail(id, sdId, serdeId,
                        PartitionInfo.builder().name(QualifiedName.ofPartition(catalogName,
                            databaseName, tableName, name)).auditInfo(auditInfo).serde(storageInfo).build()));
                }
            }
            return result;
        };
        final List<PartitionInfo> partitionInfos = new ArrayList<>();

        final List<PartitionDetail> partitions =
            getHandlerResults(databaseName, tableName, filterExpression,
                partitionIds, SQL_GET_PARTITIONS, handler, sort, pageable);
        if (includePartitionDetails && !partitions.isEmpty()) {
            final List<Long> partIds = Lists.newArrayListWithCapacity(partitions.size());
            final List<Long> sdIds = Lists.newArrayListWithCapacity(partitions.size());
            final List<Long> serdeIds = Lists.newArrayListWithCapacity(partitions.size());
            for (PartitionDetail partitionDetail : partitions) {
                partIds.add(partitionDetail.getId());
                sdIds.add(partitionDetail.getSdId());
                serdeIds.add(partitionDetail.getSerdeId());
            }
            final List<ListenableFuture<Void>> futures = Lists.newArrayList();
            final Map<Long, Map<String, String>> partitionParams = Maps.newHashMap();
            futures.add(threadServiceManager.getExecutor().submit(() ->
                populateParameters(partIds, SQL_GET_PARTITION_PARAMS,
                    "part_id", partitionParams)));

            final Map<Long, Map<String, String>> sdParams = Maps.newHashMap();
            if (!sdIds.isEmpty()) {
                futures.add(threadServiceManager.getExecutor().submit(() ->
                    populateParameters(sdIds, SQL_GET_SD_PARAMS,
                        "sd_id", sdParams)));
            }
            final Map<Long, Map<String, String>> serdeParams = Maps.newHashMap();
            if (!serdeIds.isEmpty()) {
                futures.add(threadServiceManager.getExecutor().submit(() ->
                    populateParameters(serdeIds, SQL_GET_SERDE_PARAMS,
                        "serde_id", serdeParams)));
            }
            try {
                Futures.transform(Futures.successfulAsList(futures), Functions.constant(null)).get(1, TimeUnit.HOURS);
            } catch (Exception e) {
                Throwables.propagate(e);
            }

            for (PartitionDetail partitionDetail : partitions) {
                partitionDetail.getPartitionInfo().setMetadata(partitionParams.get(partitionDetail.getId()));
                partitionDetail.getPartitionInfo().getSerde()
                    .setParameters(sdParams.get(partitionDetail.getSdId()));
                partitionDetail.getPartitionInfo().getSerde()
                    .setSerdeInfoParameters(serdeParams.get(partitionDetail.getSerdeId()));

            }
        }
        for (PartitionDetail partitionDetail : partitions) {
            partitionInfos.add(partitionDetail.getPartitionInfo());
        }
        return partitionInfos;
    }

    private <T> List<T> getHandlerResults(
        final String databaseName,
        final String tableName,
        final String filterExpression,
        final List<String> partitionIds,
        final String sql,
        final ResultSetExtractor<List<T>> resultSetExtractor,
        final Sort sort,
        final Pageable pageable
    ) {
        List<T> partitions;
        try {
            if (!Strings.isNullOrEmpty(filterExpression)) {
                final PartitionFilterGenerator generator =
                    new PartitionFilterGenerator(getPartitionKeys(databaseName, tableName));
                String filterSql = (String) new PartitionParser(new StringReader(filterExpression)).filter()
                    .jjtAccept(generator, null);
                if (generator.isOptimized()) {
                    filterSql = generator.getOptimizedSql();
                }
                if (filterSql != null && !filterSql.isEmpty()) {
                    filterSql = " and (" + filterSql + ")";
                }
                partitions = gethandlerresults(databaseName, tableName, filterExpression, partitionIds,
                    sql, resultSetExtractor,
                    generator.joinSql(), filterSql,
                    generator.getParams(), sort, pageable);
            } else {
                partitions = gethandlerresults(databaseName, tableName, null, partitionIds,
                    sql, resultSetExtractor,
                    null, null,
                    null, sort, pageable);
            }
        } catch (Exception e) {
            log.warn("Experiment: Get partitions for for table {} filter {}"
                    + " failed with error {}", tableName, filterExpression,
                e.getMessage());
            registry.counter(HiveMetrics.CounterHiveExperimentGetTablePartitionsFailure.name()).increment();

            partitions = gethandlerresults(databaseName, tableName,
                filterExpression, partitionIds, sql, resultSetExtractor, null,
                prepareFilterSql(filterExpression), Lists.newArrayList(), sort, pageable);
        }
        return partitions;
    }

    private List<FieldSchema> getPartitionKeys(final String databaseName, final String tableName) {
        final List<FieldSchema> result = Lists.newArrayList();
        final ResultSetExtractor<List<FieldSchema>> handler = rs -> {
            while (rs.next()) {
                final String name = rs.getString("pkey_name");
                final String type = rs.getString("pkey_type");
                result.add(new FieldSchema(name, type, null));
            }
            return result;
        };
        try {
            return jdbcUtil.getJdbcTemplate()
                .query(SQL_GET_PARTITION_KEYS, new Object[]{databaseName, tableName}, handler);
        } catch (DataAccessException e) {
            throw Throwables.propagate(e);
        }
    }

    private String getDateCreatedSqlCriteria(final String filterExpression) {
        final StringBuilder result = new StringBuilder();
        Collection<String> values = Lists.newArrayList();
        if (!Strings.isNullOrEmpty(filterExpression)) {
            try {
                values = (Collection<String>) new PartitionParser(new StringReader(filterExpression)).filter()
                    .jjtAccept(new PartitionParamParserEval(), null);
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
                params.putAll(getparameters(subPartitions, sql, idName)));
        } else {
            params.putAll(getparameters(ids, sql, idName));
        }
        return null;
    }

    private Map<Long, Map<String, String>> getparameters(final List<Long> ids, final String sql, final String idName) {
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
                Map<String, String> parameters = result.get(id);
                if (parameters == null) {
                    parameters = Maps.newHashMap();
                    result.put(id, parameters);
                }
                parameters.put(key, value);
            }
            return result;
        };
        try {
            return jdbcUtil.getJdbcTemplate().query(queryBuilder.toString(), handler);
        } catch (DataAccessException e) {
            Throwables.propagate(e);
        }
        return Maps.newHashMap();
    }

    private Collection<String> getSinglePartitionExprs(final String filterExpression) {
        Collection<String> result = Lists.newArrayList();
        if (!Strings.isNullOrEmpty(filterExpression)) {
            try {
                result = (Collection<String>) new PartitionParser(new StringReader(filterExpression)).filter()
                    .jjtAccept(new PartitionKeyParserEval(), null);
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

    private String prepareFilterSql(final String filterExpression) {
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

    private <T> List<T> gethandlerresults(final String databaseName,
                                          final String tableName,
                                          final String filterExpression,
                                          final List<String> partitionIds,
                                          final String sql,
                                          final ResultSetExtractor resultSetExtractor,
                                          final String joinSql,
                                          final String filterSql,
                                          final List<Object> filterParams,
                                          final Sort sort,
                                          final Pageable pageable) {
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
                    getsubhandlerresults(databaseName, tableName, filterExpression,
                        subPartitionIds, sql, resultSetExtractor,
                        joinSql, filterSql, filterParams, sort, pageable)));
        } else {
            partitions = getsubhandlerresults(databaseName, tableName, filterExpression,
                partitionIds, sql, resultSetExtractor,
                joinSql, filterSql, filterParams,
                sort, pageable);
        }
        return partitions;
    }

    private <T> List<T> getsubhandlerresults(final String databaseName,
                                             final String tableName,
                                             final String filterExpression,
                                             final List<String> partitionIds,
                                             final String sql,
                                             final ResultSetExtractor resultSetExtractor,
                                             final String joinSql,
                                             final String filterSql,
                                             final List<Object> filterParams,
                                             final Sort sort,
                                             final Pageable pageable) {
         // Create the sql
        final StringBuilder queryBuilder = new StringBuilder(sql);
        if (joinSql != null) {
            queryBuilder.append(joinSql);
        }
        queryBuilder.append(" where d.NAME = ? and t.TBL_NAME = ?");
        if (filterSql != null) {
            queryBuilder.append(filterSql);
        }
        if (partitionIds != null && !partitionIds.isEmpty()) {
            queryBuilder.append(" and p.PART_NAME in ('")
                .append(Joiner.on("','").skipNulls().join(partitionIds)).append("')");
        }
        if (sort != null && sort.hasSort()) {
            queryBuilder.append(" order by ").append(sort.getSortBy()).append(" ").append(sort.getOrder().name());
        }
        if (pageable != null && pageable.isPageable() && Strings.isNullOrEmpty(filterExpression)) {
            if (sort == null || !sort.hasSort()) {
                queryBuilder.append(" order by p.part_id");
            }
            queryBuilder.append(" limit ").append(pageable.getOffset()).append(',').append(pageable.getLimit());
        }

        List<T> partitions = Lists.newArrayList();
        try {
            final List<Object> params = Lists.newArrayList(databaseName, tableName);
            if (filterSql != null) {
                params.addAll(filterParams);
            }
            final Object[] oParams = new Object[params.size()];
            partitions = (List) jdbcUtil.getJdbcTemplate().query(
                queryBuilder.toString(), params.toArray(oParams), resultSetExtractor);
        } catch (DataAccessException e) {
            Throwables.propagate(e);
        }
        //
        if (pageable != null && pageable.isPageable() && !Strings.isNullOrEmpty(filterExpression)) {
            int limit = pageable.getOffset() + pageable.getLimit();
            if (partitions.size() < limit) {
                limit = partitions.size();
            }
            if (pageable.getOffset() > limit) {
                partitions = Lists.newArrayList();
            } else {
                partitions = partitions.subList(pageable.getOffset(), limit);
            }
        }
        return partitions;
    }

    @Override
    protected Map<String, Partition> getPartitionsByNames(final Table table, final List<String> partitionNames)
        throws TException {
        final TableInfo tableInfo = hiveMetacatConverters.toTableInfo(
            QualifiedName.ofTable(catalogName, table.getDbName(), table.getTableName()), table);
        return getpartitions(table.getDbName(), table.getTableName(),
            partitionNames, null, null, null, false)
            .stream()
            .collect(Collectors.toMap(partitionInfo -> partitionInfo.getName().getPartitionName(),
                partitionInfo -> hiveMetacatConverters.fromPartitionInfo(tableInfo, partitionInfo)));
    }
}
