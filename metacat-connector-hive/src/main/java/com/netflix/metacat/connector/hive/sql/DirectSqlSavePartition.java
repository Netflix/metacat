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
import com.google.common.collect.Lists;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.connectors.exception.ConnectorException;
import com.netflix.metacat.common.server.connectors.exception.PartitionAlreadyExistsException;
import com.netflix.metacat.common.server.connectors.exception.TableNotFoundException;
import com.netflix.metacat.common.server.connectors.model.PartitionInfo;
import com.netflix.metacat.common.server.connectors.model.StorageInfo;
import com.netflix.metacat.connector.hive.monitoring.HiveMetrics;
import com.netflix.metacat.connector.hive.util.HiveConnectorFastServiceMetric;
import com.netflix.metacat.connector.hive.util.PartitionUtil;
import com.netflix.spectator.api.Registry;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hive.metastore.api.Table;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.SqlParameterValue;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Types;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class makes direct sql calls to save hive partitions.
 *
 * @author amajumdar
 */
@Slf4j
@Transactional("hiveTxManager")
public class DirectSqlSavePartition {
    private static final String PARAM_LAST_DDL_TIME = "transient_lastDdlTime";
    private final Registry registry;
    private final String catalogName;
    private final int batchSize;
    private final JdbcTemplate jdbcTemplate;
    private final SequenceGeneration sequenceGeneration;
    private final HiveConnectorFastServiceMetric fastServiceMetric;

    /**
     * Constructor.
     *
     * @param connectorContext     connector context
     * @param jdbcTemplate         JDBC template
     * @param sequenceGeneration    sequence generator
     * @param fastServiceMetric     fast service metric
     */
    public DirectSqlSavePartition(final ConnectorContext connectorContext, final JdbcTemplate jdbcTemplate,
        final SequenceGeneration sequenceGeneration, final HiveConnectorFastServiceMetric fastServiceMetric) {
        this.registry = connectorContext.getRegistry();
        this.catalogName = connectorContext.getCatalogName();
        this.batchSize = connectorContext.getConfig().getHiveMetastoreBatchSize();
        this.jdbcTemplate = jdbcTemplate;
        this.sequenceGeneration = sequenceGeneration;
        this.fastServiceMetric = fastServiceMetric;
    }

    /**
     * Inserts the partitions.
     * Note: Column descriptor of the partitions will be set to that of the table.
     *
     * @param tableQName table name
     * @param table hive table
     * @param partitions list of partitions
     */
    public void insert(final QualifiedName tableQName, final Table table, final List<PartitionInfo> partitions) {
        final long start = registry.clock().wallTime();
        try {
            // Get the table id and column id
            final TableSequenceIds tableSequenceIds = getTableSequenceIds(table.getDbName(), table.getTableName());
            // Get the sequence ids and lock the records in the database
            final PartitionSequenceIds partitionSequenceIds =
                sequenceGeneration.newPartitionSequenceIds(partitions.size());
            final List<List<PartitionInfo>> subPartitionList = Lists.partition(partitions, batchSize);
            // Use the current time for create and update time.
            final long currentTimeInEpoch = Instant.now().getEpochSecond();
            int index = 0;
            // Insert the partitions in batches
            for (List<PartitionInfo> subPartitions : subPartitionList) {
                _insert(tableQName, table, tableSequenceIds, partitionSequenceIds, subPartitions, currentTimeInEpoch,
                    index);
                index += batchSize;
            }
        } finally {
            this.fastServiceMetric.recordTimer(
                HiveMetrics.TagAddPartitions.getMetricName(), registry.clock().wallTime() - start);
        }
    }

    @SuppressWarnings("checkstyle:methodname")
    private void _insert(final QualifiedName tableQName, final Table table, final TableSequenceIds tableSequenceIds,
        final PartitionSequenceIds partitionSequenceIds, final List<PartitionInfo> partitions,
        final long currentTimeInEpoch, final int index) {
        final List<Object[]> serdesValues = Lists.newArrayList();
        final List<Object[]> serdeParamsValues = Lists.newArrayList();
        final List<Object[]> sdsValues = Lists.newArrayList();
        final List<Object[]> partitionsValues = Lists.newArrayList();
        final List<Object[]> partitionParamsValues = Lists.newArrayList();
        final List<Object[]> partitionKeyValsValues = Lists.newArrayList();
        final List<String> partitionNames = Lists.newArrayList();
        int currentIndex = index;
        for (PartitionInfo partition: partitions) {
            final StorageInfo storageInfo = partition.getSerde();
            final long partId = partitionSequenceIds.getPartId() +  currentIndex;
            final long sdsId = partitionSequenceIds.getSdsId() +  currentIndex;
            final long serdeId = partitionSequenceIds.getSerdeId() +  currentIndex;
            final String partitionName = partition.getName().getPartitionName();
            final List<String> partValues = PartitionUtil.getPartValuesFromPartName(tableQName, table, partitionName);
            final String escapedPartName = PartitionUtil.makePartName(table.getPartitionKeys(), partValues);
            partitionsValues.add(new Object[]{0, tableSequenceIds.getTableId(), currentTimeInEpoch,
                sdsId, escapedPartName, partId, });
            for (int i = 0; i < partValues.size(); i++) {
                partitionKeyValsValues.add(new Object[] {partId, partValues.get(i), i});
            }
            if (storageInfo != null) {
                serdesValues.add(new Object[]{null, storageInfo.getSerializationLib(), serdeId});
                final Map<String, String> serdeInfoParameters = storageInfo.getSerdeInfoParameters();
                if (serdeInfoParameters != null) {
                    serdeInfoParameters
                        .forEach((key, value) -> serdeParamsValues.add(new Object[] {value, serdeId, key }));
                }
                sdsValues.add(new Object[]{storageInfo.getOutputFormat(), false, tableSequenceIds.getCdId(),
                    false, serdeId, storageInfo.getUri(), storageInfo.getInputFormat(), 0, sdsId, });
                final Map<String, String> parameters = storageInfo.getParameters();
                if (parameters != null) {
                    parameters
                        .forEach((key, value) -> partitionParamsValues.add(new Object[] {value, partId, key }));
                    partitionParamsValues.add(
                        new Object[] {currentTimeInEpoch, partId, PARAM_LAST_DDL_TIME });
                }
            }
            partitionNames.add(partitionName);
            currentIndex++;
        }
        try {
            jdbcTemplate.batchUpdate(SQL.SERDES_INSERT, serdesValues,
                new int[] {Types.VARCHAR, Types.VARCHAR, Types.BIGINT });
            jdbcTemplate.batchUpdate(SQL.SERDE_PARAMS_INSERT, serdeParamsValues,
                new int[] {Types.VARCHAR, Types.BIGINT, Types.VARCHAR });
            jdbcTemplate.batchUpdate(SQL.SDS_INSERT, sdsValues,
                new int[] {Types.VARCHAR, Types.BOOLEAN, Types.BIGINT, Types.BOOLEAN,
                    Types.BIGINT, Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.BIGINT, });
            jdbcTemplate.batchUpdate(SQL.PARTITIONS_INSERT, partitionsValues,
                new int[] {Types.INTEGER, Types.BIGINT, Types.INTEGER, Types.BIGINT, Types.VARCHAR, Types.BIGINT });
            jdbcTemplate.batchUpdate(SQL.PARTITION_PARAMS_INSERT, partitionParamsValues,
                new int[] {Types.VARCHAR, Types.BIGINT, Types.VARCHAR });
            jdbcTemplate.batchUpdate(SQL.PARTITION_KEY_VALS_INSERT, partitionKeyValsValues,
                new int[] {Types.BIGINT, Types.VARCHAR, Types.INTEGER });
        } catch (DuplicateKeyException e) {
            throw new PartitionAlreadyExistsException(tableQName, partitionNames, e);
        } catch (Exception e) {
            throw new ConnectorException(
                String.format("Failed inserting partitions %s for table %s", partitionNames, tableQName), e);
        }
    }

    private TableSequenceIds getTableSequenceIds(final String dbName, final String tableName) {
        try {
            return jdbcTemplate.queryForObject(SQL.TABLE_SELECT,
                new SqlParameterValue[] {new SqlParameterValue(Types.VARCHAR, dbName),
                    new SqlParameterValue(Types.VARCHAR, tableName), },
                (rs, rowNum) -> new TableSequenceIds(rs.getLong("tbl_id"), rs.getLong("cd_id")));
        } catch (EmptyResultDataAccessException e) {
            throw new TableNotFoundException(QualifiedName.ofTable(catalogName, dbName, tableName));
        } catch (Exception e) {
            throw new ConnectorException(String.format("Failed getting the sequence id for table %s", tableName), e);
        }
    }

    /**
     * Updates the existing partitions. This method assumes that the partitions already exists and so does not
     * validate to check if it exists.
     * Note: Column descriptor of the partitions will not be updated.
     *
     * @param tableQName  table name
     * @param partitionHolders list of partitions
     */
    public void update(final QualifiedName tableQName, final List<PartitionHolder> partitionHolders) {
        final long start = registry.clock().wallTime();
        try {
            final List<List<PartitionHolder>> subPartitionDetailList = Lists.partition(partitionHolders, batchSize);
            final long currentTimeInEpoch = Instant.now().getEpochSecond();
            for (List<PartitionHolder> subPartitionHolders : subPartitionDetailList) {
                _update(tableQName, subPartitionHolders, currentTimeInEpoch);
            }
        } finally {
            this.fastServiceMetric.recordTimer(
                HiveMetrics.TagAlterPartitions.getMetricName(), registry.clock().wallTime() - start);
        }
    }

    @SuppressWarnings("checkstyle:methodname")
    private void _update(final QualifiedName tableQName, final List<PartitionHolder> partitionHolders,
        final long currentTimeInEpoch) {
        final List<Object[]> serdesValues = Lists.newArrayList();
        final List<Object[]> serdeParamsValues = Lists.newArrayList();
        final List<Object[]> sdsValues = Lists.newArrayList();
        final List<Object[]> partitionParamsValues = Lists.newArrayList();
        final List<String> partitionNames = Lists.newArrayList();
        for (PartitionHolder partitionHolder : partitionHolders) {
            final PartitionInfo partition = partitionHolder.getPartitionInfo();
            final StorageInfo storageInfo = partition.getSerde();
            final long partId = partitionHolder.getId();
            final long sdsId = partitionHolder.getSdId();
            final long serdeId = partitionHolder.getSerdeId();
            if (storageInfo != null) {
                serdesValues.add(new Object[]{null, storageInfo.getSerializationLib(), serdeId});
                final Map<String, String> serdeInfoParameters = storageInfo.getSerdeInfoParameters();
                if (serdeInfoParameters != null) {
                    serdeInfoParameters
                        .forEach((key, value) -> serdeParamsValues.add(new Object[] {value, serdeId, key, value }));
                }
                sdsValues.add(new Object[]{storageInfo.getOutputFormat(), false, false, storageInfo.getUri(),
                    storageInfo.getInputFormat(), sdsId, });
                final Map<String, String> parameters = storageInfo.getParameters();
                if (parameters != null) {
                    parameters
                        .forEach((key, value) -> partitionParamsValues.add(new Object[] {value, partId, key, value }));
                    partitionParamsValues.add(
                        new Object[] {currentTimeInEpoch, partId, PARAM_LAST_DDL_TIME, currentTimeInEpoch });
                }
            }
            partitionNames.add(partition.getName().toString());
        }
        try {
            jdbcTemplate.batchUpdate(SQL.SERDES_UPDATE, serdesValues,
                new int[] {Types.VARCHAR, Types.VARCHAR, Types.BIGINT });
            jdbcTemplate.batchUpdate(SQL.SERDE_PARAMS_INSERT_UPDATE, serdeParamsValues,
                new int[] {Types.VARCHAR, Types.BIGINT, Types.VARCHAR, Types.VARCHAR });
            jdbcTemplate.batchUpdate(SQL.SDS_UPDATE, sdsValues,
                new int[] {Types.VARCHAR, Types.BOOLEAN, Types.BOOLEAN, Types.VARCHAR, Types.VARCHAR, Types.BIGINT });
            jdbcTemplate.batchUpdate(SQL.PARTITION_PARAMS_INSERT_UPDATE, partitionParamsValues,
                new int[] {Types.VARCHAR, Types.BIGINT, Types.VARCHAR, Types.VARCHAR });
        } catch (DuplicateKeyException e) {
            throw new PartitionAlreadyExistsException(tableQName, partitionNames, e);
        } catch (Exception e) {
            throw new ConnectorException(
                String.format("Failed updating partitions %s for table %s", partitionNames, tableQName), e);
        }
    }

    /**
     * Delete the partitions with the given <code>partitionNames</code>.
     *
     * @param tableQName table name
     * @param partitionNames list of partition ids
     */
    public void delete(final QualifiedName tableQName, final List<String> partitionNames) {
        final long start = registry.clock().wallTime();
        try {
            final List<List<String>> subPartitionNameList = Lists.partition(partitionNames, batchSize);
            subPartitionNameList.forEach(subPartitionNames -> _delete(tableQName, subPartitionNames));
        } finally {
            this.fastServiceMetric.recordTimer(
                HiveMetrics.TagDropHivePartitions.getMetricName(), registry.clock().wallTime() - start);
        }
    }

    @SuppressWarnings("checkstyle:methodname")
    private void _delete(final QualifiedName tableQName, final List<String> partitionNames) {
        try {
            final List<PartitionSequenceIds> partitionSequenceIds = getPartitionSequenceIds(tableQName, partitionNames);
            if (partitionSequenceIds != null && !partitionSequenceIds.isEmpty()) {
                _delete(partitionSequenceIds);
            }
        } catch (EmptyResultDataAccessException ignored) {
            log.debug("None of the table {} partitions {} exist for dropping.", tableQName, partitionNames, ignored);
        } catch (Exception e) {
            throw new ConnectorException(
                String.format("Failed dropping table %s partitions: %s", tableQName, partitionNames), e);
        }
    }

    private List<PartitionSequenceIds> getPartitionSequenceIds(final QualifiedName tableName,
        final List<String> partitionNames) {
        final List<String> paramVariables = partitionNames.stream().map(s -> "?").collect(Collectors.toList());
        final String paramVariableString = Joiner.on(",").skipNulls().join(paramVariables);
        final SqlParameterValue[] values = new SqlParameterValue[partitionNames.size() + 2];
        int index = 0;
        values[index++] = new SqlParameterValue(Types.VARCHAR, tableName.getDatabaseName());
        values[index++] = new SqlParameterValue(Types.VARCHAR, tableName.getTableName());
        for (String partitionName: partitionNames) {
            values[index++] = new SqlParameterValue(Types.VARCHAR, partitionName);
        }
        return jdbcTemplate.query(
            String.format(SQL.PARTITIONS_SELECT, paramVariableString), values,
            (rs, rowNum) -> new PartitionSequenceIds(rs.getLong("part_id"), rs.getLong("sd_id"),
                rs.getLong("serde_id")));
    }

    @SuppressWarnings("checkstyle:methodname")
    private void _delete(final List<PartitionSequenceIds> subPartitionIds) {
        final List<String> paramVariables = subPartitionIds.stream().map(s -> "?").collect(Collectors.toList());
        final SqlParameterValue[] partIds =
            subPartitionIds.stream().map(p -> new SqlParameterValue(Types.BIGINT, p.getPartId()))
                .toArray(SqlParameterValue[]::new);
        final SqlParameterValue[] sdsIds =
            subPartitionIds.stream().map(p -> new SqlParameterValue(Types.BIGINT, p.getSdsId()))
                .toArray(SqlParameterValue[]::new);
        final SqlParameterValue[] serdeIds =
            subPartitionIds.stream().filter(p -> p.getSerdeId() != null)
                .map(p -> new SqlParameterValue(Types.BIGINT, p.getSerdeId()))
                .toArray(SqlParameterValue[]::new);
        final String paramVariableString = Joiner.on(",").skipNulls().join(paramVariables);
        jdbcTemplate.update(
            String.format(SQL.PARTITION_KEY_VALS_DELETES, paramVariableString), (Object[]) partIds);
        jdbcTemplate.update(
            String.format(SQL.PARTITION_PARAMS_DELETES, paramVariableString), (Object[]) partIds);
        jdbcTemplate.update(
            String.format(SQL.PARTITIONS_DELETES, paramVariableString), (Object[]) partIds);
        jdbcTemplate.update(
            String.format(SQL.SERDE_PARAMS_DELETES, paramVariableString), (Object[]) serdeIds);
        jdbcTemplate.update(
            String.format(SQL.SDS_DELETES, paramVariableString), (Object[]) sdsIds);
        jdbcTemplate.update(
            String.format(SQL.SERDES_DELETES, paramVariableString), (Object[]) serdeIds);
    }

    /**
     * Drops, updates and adds partitions for a table.
     *
     * @param tableQName                table name
     * @param table                     table
     * @param addedPartitionInfos       new partitions to be added
     * @param existingPartitionHolders  existing partitions to be altered/updated
     * @param deletePartitionNames      existing partitions to be dropped
     */
    public void addUpdateDropPartitions(final QualifiedName tableQName, final Table table,
        final List<PartitionInfo> addedPartitionInfos,
        final List<PartitionHolder> existingPartitionHolders, final Set<String> deletePartitionNames) {
        final long start = registry.clock().wallTime();
        try {
            if (!deletePartitionNames.isEmpty()) {
                delete(tableQName, Lists.newArrayList(deletePartitionNames));
            }
            if (!existingPartitionHolders.isEmpty()) {
                update(tableQName, existingPartitionHolders);
            }
            if (!addedPartitionInfos.isEmpty()) {
                insert(tableQName, table, addedPartitionInfos);
            }
        } finally {
            this.fastServiceMetric.recordTimer(
                HiveMetrics.TagAddDropPartitions.getMetricName(), registry.clock().wallTime() - start);
        }
    }

    @VisibleForTesting
    private static class SQL {
        static final String SERDES_INSERT =
            "INSERT INTO SERDES (NAME,SLIB,SERDE_ID) VALUES (?,?,?)";
        static final String SERDES_UPDATE =
            "UPDATE SERDES SET NAME=?,SLIB=? WHERE SERDE_ID=?";
        static final String SERDES_DELETES =
            "DELETE FROM SERDES WHERE SERDE_ID in (%s)";
        static final String SERDE_PARAMS_INSERT =
            "INSERT INTO SERDE_PARAMS(PARAM_VALUE,SERDE_ID,PARAM_KEY) VALUES (?,?,?)";
        static final String SERDE_PARAMS_INSERT_UPDATE =
            "INSERT INTO SERDE_PARAMS(PARAM_VALUE,SERDE_ID,PARAM_KEY) VALUES (?,?,?) "
                + "ON DUPLICATE KEY UPDATE PARAM_VALUE=?";
        static final String SERDE_PARAMS_DELETES =
            "DELETE FROM SERDE_PARAMS WHERE SERDE_ID in (%s)";
        static final String SDS_INSERT =
            "INSERT INTO SDS (OUTPUT_FORMAT,IS_COMPRESSED,CD_ID,IS_STOREDASSUBDIRECTORIES,SERDE_ID,LOCATION, "
                + "INPUT_FORMAT,NUM_BUCKETS,SD_ID) VALUES (?,?,?,?,?,?,?,?,?)";
        static final String SDS_UPDATE =
            "UPDATE SDS SET OUTPUT_FORMAT=?,IS_COMPRESSED=?,IS_STOREDASSUBDIRECTORIES=?,LOCATION=?, "
                + "INPUT_FORMAT=? WHERE SD_ID=?";
        static final String SDS_DELETES =
            "DELETE FROM SDS WHERE SD_ID in (%s)";
        static final String PARTITIONS_INSERT =
            "INSERT INTO PARTITIONS(LAST_ACCESS_TIME,TBL_ID,CREATE_TIME,SD_ID,PART_NAME,PART_ID) VALUES (?,?,?,?,?,?)";
        static final String PARTITIONS_DELETES =
            "DELETE FROM PARTITIONS WHERE PART_ID in (%s)";
        static final String PARTITION_PARAMS_INSERT =
            "INSERT INTO PARTITION_PARAMS (PARAM_VALUE,PART_ID,PARAM_KEY) VALUES (?,?,?)";
        static final String PARTITION_PARAMS_INSERT_UPDATE =
            "INSERT INTO PARTITION_PARAMS (PARAM_VALUE,PART_ID,PARAM_KEY) VALUES (?,?,?) "
                + "ON DUPLICATE KEY UPDATE PARAM_VALUE=?";
        static final String PARTITION_PARAMS_DELETES =
            "DELETE FROM PARTITION_PARAMS WHERE PART_ID in (%s)";
        static final String PARTITION_KEY_VALS_INSERT =
            "INSERT INTO PARTITION_KEY_VALS(PART_ID,PART_KEY_VAL,INTEGER_IDX) VALUES (?,?,?)";
        static final String PARTITION_KEY_VALS_DELETES =
            "DELETE FROM PARTITION_KEY_VALS WHERE PART_ID in (%s)";
        static final String PARTITIONS_SELECT =
            "SELECT P.PART_ID, P.SD_ID, S.SERDE_ID FROM DBS D JOIN TBLS T ON D.DB_ID=T.DB_ID "
                + "JOIN PARTITIONS P ON T.TBL_ID=P.TBL_ID JOIN SDS S ON P.SD_ID=S.SD_ID "
                + "WHERE D.NAME=? and T.TBL_NAME=? and P.PART_NAME in (%s)";
        static final String TABLE_SELECT =
            "SELECT T.TBL_ID, S.CD_ID FROM DBS D JOIN TBLS T ON D.DB_ID=T.DB_ID JOIN SDS S ON T.SD_ID=S.SD_ID "
                + "WHERE D.NAME=? and T.TBL_NAME=?";

    }
}
