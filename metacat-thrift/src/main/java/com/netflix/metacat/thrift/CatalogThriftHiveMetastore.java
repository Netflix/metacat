/*
 *
 *  Copyright 2016 Netflix, Inc.
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
 *
 */
package com.netflix.metacat.thrift;

import com.facebook.fb303.FacebookBase;
import com.facebook.fb303.FacebookService;
import com.facebook.fb303.fb_status;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.DatabaseDto;
import com.netflix.metacat.common.dto.FieldDto;
import com.netflix.metacat.common.dto.GetPartitionsRequestDto;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.PartitionsSaveRequestDto;
import com.netflix.metacat.common.dto.StorageDto;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.exception.MetacatAlreadyExistsException;
import com.netflix.metacat.common.exception.MetacatNotFoundException;
import com.netflix.metacat.common.exception.MetacatPreconditionFailedException;
import com.netflix.metacat.common.server.api.v1.MetacatV1;
import com.netflix.metacat.common.server.api.v1.PartitionV1;
import com.netflix.metacat.common.server.monitoring.Metrics;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.spectator.api.Registry;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.AddDynamicPartitions;
import org.apache.hadoop.hive.metastore.api.AddPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.AddPartitionsResult;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.CheckLockRequest;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DropPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.DropPartitionsResult;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FireEventRequest;
import org.apache.hadoop.hive.metastore.api.FireEventResponse;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleRequest;
import org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleResponse;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalRequest;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalResponse;
import org.apache.hadoop.hive.metastore.api.GrantRevokePrivilegeRequest;
import org.apache.hadoop.hive.metastore.api.GrantRevokePrivilegeResponse;
import org.apache.hadoop.hive.metastore.api.GrantRevokeRoleRequest;
import org.apache.hadoop.hive.metastore.api.GrantRevokeRoleResponse;
import org.apache.hadoop.hive.metastore.api.HeartbeatRequest;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeRequest;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeResponse;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotificationEventRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.OpenTxnRequest;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprRequest;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprResult;
import org.apache.hadoop.hive.metastore.api.PartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.PartitionsStatsResult;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableStatsRequest;
import org.apache.hadoop.hive.metastore.api.TableStatsResult;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UnlockRequest;
import org.apache.thrift.TException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Metacat Hive thrift implementation. This uses the Metacat Resource classes.
 */
@Slf4j
public class CatalogThriftHiveMetastore extends FacebookBase
    implements FacebookService.Iface, ThriftHiveMetastore.Iface {
    private static final Joiner AND_JOINER = Joiner.on(" and ");
    private static final LoadingCache<String, Pattern> PATTERNS = CacheBuilder.newBuilder()
        .build(new CacheLoader<String, Pattern>() {
            public Pattern load(
                @Nonnull final String regex) {
                return Pattern.compile(regex);
            }
        });
    private final String catalogName;
    private final Config config;
    private final HiveConverters hiveConverters;
    private final PartitionV1 partV1;
    private final MetacatV1 v1;
    private final Map<String, List<PrivilegeGrantInfo>> defaultRolesPrivilegeSet =
        Maps.newHashMap(ImmutableMap.of("users",
            Lists.newArrayList(new PrivilegeGrantInfo("ALL", 0, "hadoop", PrincipalType.ROLE, true))));
    private final Registry registry;

    /**
     * Constructor.
     *
     * @param config         config
     * @param hiveConverters hive converter
     * @param metacatV1      Metacat V1 resource
     * @param partitionV1    Partition V1 resource
     * @param catalogName    catalog name
     * @param registry       registry of spectator
     */
    public CatalogThriftHiveMetastore(
        final Config config,
        final HiveConverters hiveConverters,
        final MetacatV1 metacatV1,
        final PartitionV1 partitionV1,
        final String catalogName,
        final Registry registry
    ) {
        super("CatalogThriftHiveMetastore");

        this.config = Preconditions.checkNotNull(config, "config is null");
        this.hiveConverters = Preconditions.checkNotNull(hiveConverters, "hive converters is null");
        this.v1 = Preconditions.checkNotNull(metacatV1, "metacat api is null");
        this.partV1 = Preconditions.checkNotNull(partitionV1, "partition api is null");
        this.catalogName = normalizeIdentifier(Preconditions.checkNotNull(catalogName, "catalog name is required"));
        this.registry = registry;
    }

    private static String normalizeIdentifier(@Nullable final String s) {
        if (s == null) {
            return null;
        } else {
            return s.trim().toLowerCase(Locale.ENGLISH);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void abort_txn(final AbortTxnRequest rqst) throws TException {
        throw unimplemented("abort_txn", new Object[]{rqst});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void add_dynamic_partitions(final AddDynamicPartitions rqst) throws TException {
        throw unimplemented("add_dynamic_partitions", new Object[]{rqst});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Index add_index(final Index newIndex, final Table indexTable) throws TException {
        throw unimplemented("add_index", new Object[]{newIndex, indexTable});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Partition add_partition(final Partition newPart) throws TException {
        return add_partition_with_environment_context(newPart, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Partition add_partition_with_environment_context(
        final Partition newPart,
        @Nullable final EnvironmentContext ec
    ) throws TException {
        final String dbName = normalizeIdentifier(newPart.getDbName());
        final String tableName = normalizeIdentifier(newPart.getTableName());
        return requestWrapper("add_partition_with_environment_context", new Object[]{dbName, tableName, ec}, () -> {
            addPartitionsCore(dbName, tableName, ImmutableList.of(newPart), false);
            return newPart;
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int add_partitions(final List<Partition> newParts) throws TException {
        if (newParts == null || newParts.size() == 0) {
            return 0;
        }
        final String dbName = normalizeIdentifier(newParts.get(0).getDbName());
        final String tableName = normalizeIdentifier(newParts.get(0).getTableName());
        return requestWrapper("add_partition", new Object[]{dbName, tableName}, () -> {
            addPartitionsCore(dbName, tableName, newParts, false);
            return newParts.size();
        });
    }

    @Override
    public int add_partitions_pspec(final List<PartitionSpec> newParts) throws TException {
        throw unimplemented("add_partitions_pspec", new Object[]{newParts});
    }

    @Override
    public AddPartitionsResult add_partitions_req(final AddPartitionsRequest request) throws TException {
        final String dbName = normalizeIdentifier(request.getDbName());
        final String tableName = normalizeIdentifier(request.getTblName());
        return requestWrapper("add_partition", new Object[]{dbName, tableName}, () -> {
            final List<Partition> partitions = addPartitionsCore(dbName, tableName, request.getParts(),
                request.isIfNotExists());
            final AddPartitionsResult result = new AddPartitionsResult();
            result.setPartitions(partitions);
            return result;
        });
    }

    private List<Partition> addPartitionsCore(final String dbName, final String tblName, final List<Partition> parts,
                                              final boolean ifNotExists)
        throws TException {
        log.debug("Ignoring {} since metacat save partitions will do an update if it already exists", ifNotExists);
        final TableDto tableDto = v1.getTable(catalogName, dbName, tblName, true, false, false);
        if (tableDto.getPartition_keys() == null || tableDto.getPartition_keys().isEmpty()) {
            throw new MetaException("Unable to add partition to unpartitioned table: " + tableDto.getName());
        }

        final PartitionsSaveRequestDto partitionsSaveRequestDto = new PartitionsSaveRequestDto();
        final List<PartitionDto> converted = Lists.newArrayListWithCapacity(parts.size());
        for (Partition partition : parts) {
            converted.add(hiveConverters.hiveToMetacatPartition(tableDto, partition));
        }
        partitionsSaveRequestDto.setPartitions(converted);
        partV1.savePartitions(catalogName, dbName, tblName, partitionsSaveRequestDto);
        return parts;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void alter_database(final String dbname, final Database db) throws TException {
        throw unimplemented("alter_database", new Object[]{dbname, db});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void alter_function(final String dbName, final String funcName, final Function newFunc) throws TException {
        throw unimplemented("alter_function", new Object[]{dbName, funcName, newFunc});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void alter_index(final String dbname, final String baseTblName, final String idxName,
                            final Index newIdx) throws TException {
        throw unimplemented("alter_index", new Object[]{dbname, baseTblName, idxName, newIdx});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void alter_partition(final String dbName, final String tblName, final Partition newPart) throws TException {
        alter_partition_with_environment_context(dbName, tblName, newPart, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void alter_partition_with_environment_context(
        final String dbName,
        final String tblName,
        final Partition newPart,
        @Nullable final EnvironmentContext ec
    ) throws TException {
        final String databaseName = normalizeIdentifier(dbName);
        final String tableName = normalizeIdentifier(tblName);
        requestWrapper("alter_partition_with_environment_context", new Object[]{databaseName, tableName, ec},
            () -> {
                addPartitionsCore(dbName, tableName, ImmutableList.of(newPart), false);
                return null;
            });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void alter_partitions(final String dbName, final String tblName, final List<Partition> newParts)
        throws TException {
        final String databaseName = normalizeIdentifier(dbName);
        final String tableName = normalizeIdentifier(tblName);
        requestWrapper("add_partition", new Object[]{databaseName, tableName}, () -> {
            addPartitionsCore(dbName, tableName, newParts, false);
            return null;
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void alter_table(final String dbname, final String tblName, final Table newTbl) throws TException {
        alter_table_with_environment_context(dbname, tblName, newTbl, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void alter_table_with_cascade(
        final String dbname,
        final String tblName,
        final Table newTbl,
        final boolean cascade
    ) throws TException {
        //TODO: Add logic to cascade the changes to the partitions
        alter_table_with_environment_context(dbname, tblName, newTbl, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void alter_table_with_environment_context(
        final String dbname,
        final String tblName,
        final Table newTbl,
        @Nullable final EnvironmentContext environmentContext
    ) throws TException {
        requestWrapper("alter_table_with_environment_context",
            new Object[]{dbname, tblName, newTbl, environmentContext}, () -> {
                final String databaseName = normalizeIdentifier(dbname);
                final String tableName = normalizeIdentifier(tblName);
                final QualifiedName oldName = QualifiedName.ofTable(catalogName, databaseName, tableName);
                final QualifiedName newName = QualifiedName
                    .ofTable(catalogName, newTbl.getDbName(), newTbl.getTableName());

                final TableDto dto = hiveConverters.hiveToMetacatTable(newName, newTbl);
                if (!oldName.equals(newName)) {
                    v1.renameTable(catalogName, oldName.getDatabaseName(), oldName.getTableName(),
                        newName.getTableName());
                }
                v1.updateTable(catalogName, dbname, newName.getTableName(), dto);
                return null;
            });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Partition append_partition(final String dbName, final String tblName, final List<String> partVals)
        throws TException {
        return append_partition_with_environment_context(dbName, tblName, partVals, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Partition append_partition_by_name(final String dbName, final String tblName, final String partName)
        throws TException {
        return append_partition_by_name_with_environment_context(dbName, tblName, partName, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Partition append_partition_by_name_with_environment_context(
        final String dbName, final String tblName,
        final String partName,
        @Nullable final EnvironmentContext environmentContext
    ) throws TException {
        return requestWrapper("append_partition_by_name_with_environment_context",
            new Object[]{dbName, tblName, partName},
            () -> appendPartitionsCoreAndReturn(dbName, tblName, partName));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Partition append_partition_with_environment_context(
        final String dbName,
        final String tblName,
        final List<String> partVals,
        @Nullable final EnvironmentContext environmentContext
    ) throws TException {
        return requestWrapper("append_partition_by_name_with_environment_context",
            new Object[]{dbName, tblName, partVals}, () -> {
                final TableDto tableDto = getTableDto(dbName, tblName);
                final String partName = hiveConverters.getNameFromPartVals(tableDto, partVals);
                appendPartitionsCore(dbName, tblName, partName);
                return hiveConverters.metacatToHivePartition(getPartitionDtoByName(tableDto, partName), tableDto);
            });
    }

    private void appendPartitionsCore(final String dbName, final String tblName, final String partName)
        throws TException {
        final PartitionsSaveRequestDto partitionsSaveRequestDto = new PartitionsSaveRequestDto();
        final PartitionDto partitionDto = new PartitionDto();
        partitionDto.setName(QualifiedName.ofPartition(catalogName, dbName, tblName, partName));
        partitionDto.setSerde(new StorageDto());
        partitionsSaveRequestDto.setPartitions(Lists.newArrayList(partitionDto));
        partV1.savePartitions(catalogName, dbName, tblName, partitionsSaveRequestDto);
    }

    private Partition appendPartitionsCoreAndReturn(final String dbName, final String tblName, final String partName)
        throws TException {
        appendPartitionsCore(dbName, tblName, partName);
        return getPartitionByName(dbName, tblName, partName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void cancel_delegation_token(final String tokenStrForm) throws TException {
        throw unimplemented("cancel_delegation_token", new Object[]{tokenStrForm});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LockResponse check_lock(final CheckLockRequest rqst) throws TException {
        throw unimplemented("check_lock", new Object[]{rqst});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void commit_txn(final CommitTxnRequest rqst) throws TException {
        throw unimplemented("commit_txn", new Object[]{rqst});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void compact(final CompactionRequest rqst) throws TException {
        throw unimplemented("compact", new Object[]{rqst});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void create_database(final Database database) throws TException {
        requestWrapper("create_database", new Object[]{database}, () -> {
            final String dbName = normalizeIdentifier(database.getName());
            v1.createDatabase(catalogName, dbName, null);
            return null;
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void create_function(final Function func) throws TException {
        throw unimplemented("create_function", new Object[]{func});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean create_role(final Role role) throws TException {
        throw unimplemented("create_role", new Object[]{role});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void create_table(final Table tbl) throws TException {
        create_table_with_environment_context(tbl, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void create_table_with_environment_context(
        final Table tbl,
        @Nullable final EnvironmentContext environmentContext
    ) throws TException {
        requestWrapper("create_table_with_environment_context", new Object[]{tbl, environmentContext}, () -> {
            final String dbname = normalizeIdentifier(tbl.getDbName());
            final String tblName = normalizeIdentifier(tbl.getTableName());
            final QualifiedName name = QualifiedName.ofTable(catalogName, dbname, tblName);

            final TableDto dto = hiveConverters.hiveToMetacatTable(name, tbl);
            v1.createTable(catalogName, dbname, tblName, dto);
            return null;
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean create_type(final Type type) throws TException {
        throw unimplemented("create_type", new Object[]{type});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean delete_partition_column_statistics(final String dbName, final String tblName,
                                                      final String partName, final String colName) throws TException {
        throw unimplemented("delete_partition_column_statistics",
            new Object[]{dbName, tblName, partName, colName});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean delete_table_column_statistics(final String dbName, final String tblName, final String colName)
        throws TException {
        throw unimplemented("delete_table_column_statistics", new Object[]{dbName, tblName, colName});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void drop_database(final String name, final boolean deleteData, final boolean cascade) throws TException {
        throw unimplemented("drop_database", new Object[]{name, deleteData, cascade});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void drop_function(final String dbName, final String funcName) throws TException {
        throw unimplemented("drop_function", new Object[]{dbName, funcName});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean drop_index_by_name(final String dbName, final String tblName, final String indexName,
                                      final boolean deleteData)
        throws TException {
        throw unimplemented("drop_index_by_name", new Object[]{dbName, tblName, indexName, deleteData});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean drop_partition(final String dbName, final String tblName, final List<String> partVals,
                                  final boolean deleteData)
        throws TException {
        return drop_partition_with_environment_context(dbName, tblName, partVals, deleteData, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean drop_partition_by_name(final String dbName, final String tblName, final String partName,
                                          final boolean deleteData)
        throws TException {
        return drop_partition_by_name_with_environment_context(dbName, tblName, partName, deleteData, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean drop_partition_by_name_with_environment_context(
        final String dbName, final String tblName,
        final String partName,
        final boolean deleteData,
        @Nullable final EnvironmentContext environmentContext
    ) throws TException {
        return requestWrapper("drop_partition_by_name_with_environment_context",
            new Object[]{dbName, tblName, partName, deleteData, environmentContext}, () -> {
                final String databaseName = normalizeIdentifier(dbName);
                final String tableName = normalizeIdentifier(tblName);

                if (deleteData) {
                    log.warn("Ignoring command to delete data for {}/{}/{}/{}",
                        catalogName, databaseName, tableName, partName);
                }

                partV1.deletePartitions(catalogName, databaseName, tableName, ImmutableList.of(partName));

                return true;
            });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean drop_partition_with_environment_context(
        final String dbName, final String tblName,
        final List<String> partVals,
        final boolean deleteData,
        @Nullable final EnvironmentContext environmentContext
    ) throws TException {
        return requestWrapper("drop_partition_with_environment_context",
            new Object[]{dbName, tblName, partVals, deleteData, environmentContext}, () -> {
                final TableDto tableDto = getTableDto(dbName, tblName);
                final String partName = hiveConverters.getNameFromPartVals(tableDto, partVals);

                final QualifiedName partitionName = getPartitionDtoByName(tableDto, partName).getName();

                if (deleteData) {
                    log.warn("Ignoring command to delete data for {}/{}/{}/{}",
                        catalogName, tableDto.getName().getDatabaseName(), tableDto.getName().getTableName(),
                        partitionName.getPartitionName());
                }

                partV1.deletePartitions(
                    catalogName, tableDto.getName().getDatabaseName(), tableDto.getName().getTableName(),
                    ImmutableList.of(partitionName.getPartitionName()));

                return true;
            });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DropPartitionsResult drop_partitions_req(final DropPartitionsRequest req) throws TException {
        throw unimplemented("drop_partitions_req", new Object[]{req});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean drop_role(final String roleName) throws TException {
        throw unimplemented("drop_role", new Object[]{roleName});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void drop_table(final String dbname, final String name, final boolean deleteData) throws TException {
        drop_table_with_environment_context(dbname, name, deleteData, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void drop_table_with_environment_context(final String dbname, final String name,
                                                    final boolean deleteData,
                                                    @Nullable final EnvironmentContext ec) throws TException {
        requestWrapper("drop_table_with_environment_context", new Object[]{dbname, name, deleteData, ec}, () -> {
            final String databaseName = normalizeIdentifier(dbname);
            final String tableName = normalizeIdentifier(name);

            if (deleteData) {
                log.warn("Ignoring command to delete data for {}/{}/{}", catalogName, databaseName, tableName);
            }

            return v1.deleteTable(catalogName, databaseName, tableName);
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean drop_type(final String type) throws TException {
        throw unimplemented("drop_type", new Object[]{type});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Partition exchange_partition(final Map<String, String> partitionSpecs, final String sourceDb,
                                        final String sourceTableName,
                                        final String destDb, final String destTableName) throws TException {
        throw unimplemented("exchange_partition",
            new Object[]{partitionSpecs, sourceDb, sourceTableName, destDb, destTableName});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FireEventResponse fire_listener_event(final FireEventRequest rqst) throws TException {
        throw unimplemented("fire_listener_event", new Object[]{rqst});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getCpuProfile(final int i) throws TException {
        return "";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getMetaConf(final String key) throws TException {
        throw unimplemented("getMetaConf", new Object[]{key});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public fb_status getStatus() {
        log.info("Thrift({}): Called getStatus", catalogName);
        return fb_status.ALIVE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getVersion() throws TException {
        log.info("Thrift({}): Called getVersion", catalogName);
        return "3.0";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AggrStats get_aggr_stats_for(final PartitionsStatsRequest request) throws TException {
        throw unimplemented("get_aggr_stats_for", new Object[]{request});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> get_all_databases() throws TException {
        return requestWrapper("get_all_databases", new Object[]{}, () -> v1.getCatalog(catalogName).getDatabases());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> get_all_tables(final String dbName) throws TException {
        final String databaseName = normalizeIdentifier(dbName);
        return requestWrapper("get_all_tables", new Object[]{dbName},
            () -> v1.getDatabase(catalogName, databaseName, false, true).getTables());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String get_config_value(final String name, final String defaultValue) throws TException {
        throw unimplemented("get_config_value", new Object[]{name, defaultValue});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CurrentNotificationEventId get_current_notificationEventId() throws TException {
        throw unimplemented("get_current_notificationEventId", new Object[]{});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Database get_database(final String name) throws TException {
        return requestWrapper("get_database", new Object[]{name}, () -> {
            final String databaseName = normalizeIdentifier(name);
            final DatabaseDto dto = v1.getDatabase(catalogName, databaseName, true, false);
            return hiveConverters.metacatToHiveDatabase(dto);
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> get_databases(final String hivePattern) throws TException {
        return requestWrapper("get_databases", new Object[]{hivePattern}, () -> {
            List<String> result = v1.getCatalog(catalogName).getDatabases();
            if (hivePattern != null) {
                // Unsure about the pattern format.  I couldn't find any tests.  Assuming it is regex.
                final Pattern pattern = PATTERNS.getUnchecked("(?i)" + hivePattern.replaceAll("\\*", ".*"));
                result = result.stream().filter(name -> pattern.matcher(name).matches())
                    .collect(Collectors.toList());
            }
            return result;
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String get_delegation_token(final String tokenOwner, final String renewerKerberosPrincipalName)
        throws TException {
        throw unimplemented("get_delegation_token", new Object[]{tokenOwner, renewerKerberosPrincipalName});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<FieldSchema> get_fields(final String dbName, final String tableName) throws TException {
        return get_fields_with_environment_context(dbName, tableName, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<FieldSchema> get_fields_with_environment_context(
        final String dbName,
        final String tableName,
        @Nullable final EnvironmentContext environmentContext
    ) throws TException {
        return requestWrapper("get_fields_with_environment_context",
            new Object[]{dbName, tableName, environmentContext}, () -> {
                final Table table = get_table(dbName, tableName);

                if (table == null || table.getSd() == null || table.getSd().getCols() == null) {
                    throw new MetaException("Unable to get fields for " + dbName + "." + tableName);
                }
                return table.getSd().getCols();
            });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Function get_function(final String dbName, final String funcName) throws TException {
        throw unimplemented("get_function", new Object[]{dbName, funcName});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> get_functions(final String dbName, final String pattern) throws TException {
        return Collections.emptyList();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Index get_index_by_name(final String dbName, final String tblName, final String indexName)
        throws TException {
        throw unimplemented("get_index_by_name", new Object[]{dbName, tblName, indexName});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> get_index_names(final String dbName, final String tblName, final short maxIndexes)
        throws TException {
        throw unimplemented("get_index_names", new Object[]{dbName, tblName, maxIndexes});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Index> get_indexes(final String dbName, final String tblName, final short maxIndexes)
        throws TException {
        throw unimplemented("get_indexes", new Object[]{dbName, tblName, maxIndexes});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NotificationEventResponse get_next_notification(final NotificationEventRequest rqst) throws TException {
        throw unimplemented("get_next_notification", new Object[]{rqst});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GetOpenTxnsResponse get_open_txns() throws TException {
        throw unimplemented("get_open_txns", new Object[]{});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GetOpenTxnsInfoResponse get_open_txns_info() throws TException {
        throw unimplemented("get_open_txns_info", new Object[]{});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<PartitionSpec> get_part_specs_by_filter(final String dbName, final String tblName,
                                                        final String filter, final int maxParts)
        throws TException {
        throw unimplemented("get_part_specs_by_filter", new Object[]{dbName, tblName, filter, maxParts});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Partition get_partition(final String dbName, final String tblName, final List<String> partVals)
        throws TException {
        return requestWrapper("get_partition", new Object[]{dbName, tblName, partVals}, () -> {
            final TableDto tableDto = getTableDto(dbName, tblName);
            final String partName = hiveConverters.getNameFromPartVals(tableDto, partVals);
            return hiveConverters.metacatToHivePartition(getPartitionDtoByName(tableDto, partName), tableDto);
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Partition get_partition_by_name(final String dbName, final String tblName, final String partName)
        throws TException {
        return requestWrapper("get_partition_by_name", new Object[]{dbName, tblName, partName},
            () -> getPartitionByName(dbName, tblName, partName));
    }

    private TableDto getTableDto(final String dbName, final String tblName) {
        final String databaseName = normalizeIdentifier(dbName);
        final String tableName = normalizeIdentifier(tblName);
        return v1.getTable(catalogName, databaseName, tableName, true, false, false);
    }

    private Partition getPartitionByName(final String dbName, final String tblName, final String partName
    ) throws TException {
        final TableDto tableDto = getTableDto(dbName, tblName);
        return hiveConverters.metacatToHivePartition(getPartitionDtoByName(tableDto, partName), tableDto);
    }

    private PartitionDto getPartitionDtoByName(final TableDto tableDto, final String partName) throws TException {
        final GetPartitionsRequestDto dto = new GetPartitionsRequestDto(null, ImmutableList.of(partName), true, true);
        final List<PartitionDto> partitionDtos = partV1.getPartitionsForRequest(
            catalogName, tableDto.getName().getDatabaseName(), tableDto.getName().getTableName(), null, null, null,
            null, false, dto);

        if (partitionDtos == null || partitionDtos.isEmpty()) {
            throw new NoSuchObjectException("Partition (" + partName + ") not found on " + tableDto.getName());
        } else if (partitionDtos.size() != 1) {
            // I don't think this is even possible
            throw new NoSuchObjectException("Partition (" + partName + ") matched extra on " + tableDto.getName());
        }
        return partitionDtos.get(0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ColumnStatistics get_partition_column_statistics(
        final String dbName,
        final String tblName,
        final String partName,
        final String colName
    ) throws TException {
        throw unimplemented("get_partition_column_statistics", new Object[]{dbName, tblName, partName, colName});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> get_partition_names(final String dbName, final String tblName, final short maxParts)
        throws TException {
        final String databaseName = normalizeIdentifier(dbName);
        final String tableName = normalizeIdentifier(tblName);
        final Integer maxValues = maxParts > 0 ? Short.toUnsignedInt(maxParts) : null;
        return requestWrapper("get_partition_names", new Object[]{databaseName, tableName, maxParts}, () -> partV1
            .getPartitionKeys(catalogName, databaseName, tableName, null, null, null, null, maxValues));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> get_partition_names_ps(final String dbName, final String tblName,
                                               final List<String> partVals, final short maxParts) throws TException {
        return requestWrapper("get_partition_names_ps", new Object[]{dbName, tblName, partVals, maxParts},
            () -> {
                final String databaseName = normalizeIdentifier(dbName);
                final String tableName = normalizeIdentifier(tblName);
                final String partFilter = partition_values_to_partition_filter(databaseName, tableName, partVals);

                final Integer maxValues = maxParts > 0 ? Short.toUnsignedInt(maxParts) : null;
                return partV1.getPartitionKeys(catalogName, databaseName, tableName, partFilter, null, null, null,
                    maxValues);
            });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Partition get_partition_with_auth(
        final String dbName,
        final String tblName,
        final List<String> partVals,
        final String userName,
        final List<String> groupNames
    ) throws TException {
        //TODO: Handle setting the privileges
        return get_partition(dbName, tblName, partVals);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Partition> get_partitions(final String dbName, final String tblName, final short maxParts)
        throws TException {
        return requestWrapper("get_partitions", new Object[]{dbName, tblName, maxParts}, () -> {
            final String databaseName = normalizeIdentifier(dbName);
            final String tableName = normalizeIdentifier(tblName);
            final TableDto tableDto = v1.getTable(catalogName, databaseName, tableName, true, false, false);

            final Integer maxValues = maxParts > 0 ? Short.toUnsignedInt(maxParts) : null;
            final List<PartitionDto> metacatPartitions = partV1.getPartitions(catalogName, dbName, tblName, null, null,
                null, null, maxValues, false);
            final List<Partition> result = Lists.newArrayListWithCapacity(metacatPartitions.size());
            for (PartitionDto partition : metacatPartitions) {
                result.add(hiveConverters.metacatToHivePartition(partition, tableDto));
            }
            return result;
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PartitionsByExprResult get_partitions_by_expr(final PartitionsByExprRequest req) throws TException {
        throw unimplemented("get_partitions_by_expr", new Object[]{req});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Partition> get_partitions_by_filter(final String dbName, final String tblName, final String filter,
                                                    final short maxParts)
        throws TException {
        return requestWrapper("get_partitions_by_filter", new Object[]{dbName, tblName, filter, maxParts},
            () -> {
                final String databaseName = normalizeIdentifier(dbName);
                final String tableName = normalizeIdentifier(tblName);
                final TableDto tableDto = v1.getTable(catalogName, databaseName, tableName, true, false, false);

                final Integer maxValues = maxParts > 0 ? Short.toUnsignedInt(maxParts) : null;
                final List<PartitionDto> metacatPartitions = partV1.getPartitions(catalogName, dbName, tblName,
                    filter, null,
                    null, null, maxValues, false);
                final List<Partition> result = Lists.newArrayListWithCapacity(metacatPartitions.size());
                for (PartitionDto partition : metacatPartitions) {
                    result.add(hiveConverters.metacatToHivePartition(partition, tableDto));
                }
                return result;
            });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Partition> get_partitions_by_names(final String dbName, final String tblName,
                                                   final List<String> names)
        throws TException {
        return requestWrapper("get_partitions_by_names", new Object[]{dbName, tblName, names}, () -> {
            final String databaseName = normalizeIdentifier(dbName);
            final String tableName = normalizeIdentifier(tblName);
            final TableDto tableDto = v1.getTable(catalogName, databaseName, tableName, true, false, false);
            final GetPartitionsRequestDto dto = new GetPartitionsRequestDto(null, names, true, true);
            final List<PartitionDto> metacatPartitions =
                partV1.getPartitionsForRequest(catalogName, databaseName, tableName, null,
                    null, null, null, false, dto);
            final List<Partition> result = Lists.newArrayListWithCapacity(metacatPartitions.size());
            for (PartitionDto partition : metacatPartitions) {
                result.add(hiveConverters.metacatToHivePartition(partition, tableDto));
            }
            return result;
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Partition> get_partitions_ps(final String dbName, final String tblName,
                                             final List<String> partVals, final short maxParts)
        throws TException {
        return requestWrapper("get_partitions_ps", new Object[]{dbName, tblName, partVals, maxParts}, () -> {
            final String databaseName = normalizeIdentifier(dbName);
            final String tableName = normalizeIdentifier(tblName);
            final TableDto tableDto = v1.getTable(catalogName, databaseName, tableName, true, false, false);
            final String partFilter = partition_values_to_partition_filter(tableDto, partVals);

            final Integer maxValues = maxParts > 0 ? Short.toUnsignedInt(maxParts) : null;
            final List<PartitionDto> metacatPartitions = partV1.getPartitions(catalogName, dbName, tblName, partFilter,
                null, null, null, maxValues, false);
            final List<Partition> result = Lists.newArrayListWithCapacity(metacatPartitions.size());
            for (PartitionDto partition : metacatPartitions) {
                result.add(hiveConverters.metacatToHivePartition(partition, tableDto));
            }
            return result;
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Partition> get_partitions_ps_with_auth(
        final String dbName, final String tblName,
        final List<String> partVals,
        final short maxParts,
        final String userName,
        final List<String> groupNames
    ) throws TException {
        //TODO: Handle setting the privileges
        return get_partitions_ps(dbName, tblName, partVals, maxParts);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<PartitionSpec> get_partitions_pspec(final String dbName, final String tblName, final int maxParts)
        throws TException {
        throw unimplemented("get_partitions_pspec", new Object[]{dbName, tblName, maxParts});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PartitionsStatsResult get_partitions_statistics_req(final PartitionsStatsRequest request) throws TException {
        throw unimplemented("get_partitions_statistics_req", new Object[]{request});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Partition> get_partitions_with_auth(
        final String dbName,
        final String tblName,
        final short maxParts,
        final String userName,
        final List<String> groupNames
    ) throws TException {
        //TODO: Handle setting the privileges
        return get_partitions(dbName, tblName, maxParts);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GetPrincipalsInRoleResponse get_principals_in_role(final GetPrincipalsInRoleRequest request)
        throws TException {
        throw unimplemented("get_principals_in_role", new Object[]{request});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PrincipalPrivilegeSet get_privilege_set(final HiveObjectRef hiveObject, final String userName,
                                                   final List<String> groupNames)
        throws TException {
        return requestWrapper("get_privilege_set", new Object[]{hiveObject, userName, groupNames},
            () -> {
                Map<String, List<PrivilegeGrantInfo>> groupPrivilegeSet = null;
                Map<String, List<PrivilegeGrantInfo>> userPrivilegeSet = null;

                if (groupNames != null) {
                    groupPrivilegeSet = groupNames.stream()
                        .collect(Collectors.toMap(p -> p, p -> Lists.newArrayList()));
                }
                if (userName != null) {
                    userPrivilegeSet = ImmutableMap.of(userName, Lists.newArrayList());
                }
                return new PrincipalPrivilegeSet(userPrivilegeSet,
                    groupPrivilegeSet,
                    defaultRolesPrivilegeSet);
            });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GetRoleGrantsForPrincipalResponse get_role_grants_for_principal(
        final GetRoleGrantsForPrincipalRequest request)
        throws TException {
        throw unimplemented("get_role_grants_for_principal", new Object[]{request});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> get_role_names() throws TException {
        throw unimplemented("get_role_names", new Object[]{});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<FieldSchema> get_schema(final String dbName, final String tableName) throws TException {
        return get_schema_with_environment_context(dbName, tableName, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<FieldSchema> get_schema_with_environment_context(
        final String dbName,
        final String tableName,
        @Nullable final EnvironmentContext environmentContext
    ) throws TException {
        return requestWrapper("get_schema_with_environment_context",
            new Object[]{dbName, tableName, environmentContext}, () -> {
                final Table table = get_table(dbName, tableName);
                List<FieldSchema> partitionKeys = Collections.emptyList();
                List<FieldSchema> columns = Collections.emptyList();

                if (table != null && table.getSd() != null && table.getSd().getCols() != null) {
                    columns = table.getSd().getCols();
                }

                if (table != null && table.getPartitionKeys() != null) {
                    partitionKeys = table.getPartitionKeys();
                }

                if (partitionKeys.isEmpty() && columns.isEmpty()) {
                    throw new MetaException(
                        "Table does not have any partition keys or cols: " + dbName + "." + tableName);
                }

                final List<FieldSchema> result = Lists.newArrayListWithCapacity(partitionKeys.size() + columns.size());
                result.addAll(columns);
                result.addAll(partitionKeys);
                return result;
            });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Table get_table(final String dbname, final String tblName) throws TException {
        return requestWrapper("get_table", new Object[]{dbname, tblName}, () -> {
            final String databaseName = normalizeIdentifier(dbname);
            final String tableName = normalizeIdentifier(tblName);

            final TableDto dto = v1.getTable(catalogName, databaseName, tableName, true, true, true);
            return hiveConverters.metacatToHiveTable(dto);
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ColumnStatistics get_table_column_statistics(final String dbName, final String tblName,
                                                        final String colName)
        throws TException {
        throw unimplemented("get_table_column_statistics", new Object[]{dbName, tblName, colName});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> get_table_names_by_filter(final String dbname, final String filter, final short maxTables)
        throws TException {
        return requestWrapper("get_table_names_by_filter", new Object[]{dbname, filter, maxTables}, () -> {
            final String databaseName = normalizeIdentifier(dbname);

            final List<String> tables =
                v1.getDatabase(catalogName, databaseName, false, true).getTables();
            // TODO apply filter
            if (!"hive_filter_field_params__presto_view = \"true\"".equals(filter)) {
                throw new IllegalArgumentException("Unexpected filter: '" + filter + "'");
            }
            if (maxTables <= 0 || maxTables >= tables.size()) {
                return tables;
            } else {
                return tables.subList(0, maxTables);
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Table> get_table_objects_by_name(final String dbname, final List<String> tblNames) throws TException {
        return requestWrapper("get_table_objects_by_name", new Object[]{dbname, tblNames}, () -> {
            final String databaseName = normalizeIdentifier(dbname);
            return tblNames.stream()
                .map(CatalogThriftHiveMetastore::normalizeIdentifier)
                .map(name -> v1.getTable(catalogName, databaseName, name, true, true, true))
                .map(this.hiveConverters::metacatToHiveTable)
                .collect(Collectors.toList());
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TableStatsResult get_table_statistics_req(final TableStatsRequest request) throws TException {
        throw unimplemented("get_table_statistics_req", new Object[]{request});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> get_tables(final String dbName, final String hivePattern) throws TException {
        return requestWrapper("get_tables", new Object[]{dbName, hivePattern}, () -> {
            List<String> result = v1.getDatabase(catalogName, dbName, false, true).getTables();
            if (hivePattern != null) {
                final Pattern pattern = PATTERNS.getUnchecked("(?i)" + hivePattern.replaceAll("\\*", ".*"));
                result = result.stream().filter(name -> pattern.matcher(name).matches()).collect(Collectors.toList());
            }
            return result;
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Type get_type(final String name) throws TException {
        throw unimplemented("get_type", new Object[]{name});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Type> get_type_all(final String name) throws TException {
        throw unimplemented("get_type_all", new Object[]{name});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean grant_privileges(final PrivilegeBag privileges) throws TException {
        throw unimplemented("grant_privileges", new Object[]{privileges});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GrantRevokePrivilegeResponse grant_revoke_privileges(final GrantRevokePrivilegeRequest request)
        throws TException {
        throw unimplemented("grant_revoke_privileges", new Object[]{request});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GrantRevokeRoleResponse grant_revoke_role(final GrantRevokeRoleRequest request) throws TException {
        throw unimplemented("grant_revoke_role", new Object[]{request});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean grant_role(
        final String roleName,
        final String principalName,
        final PrincipalType principalType,
        final String grantor,
        final PrincipalType grantorType,
        final boolean grantOption
    ) throws TException {
        throw unimplemented("grant_role",
            new Object[]{roleName, principalName, principalType, grantor, grantorType});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void heartbeat(final HeartbeatRequest ids) throws TException {
        throw unimplemented("heartbeat", new Object[]{ids});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public HeartbeatTxnRangeResponse heartbeat_txn_range(final HeartbeatTxnRangeRequest txns) throws TException {
        throw unimplemented("heartbeat_txn_range", new Object[]{txns});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isPartitionMarkedForEvent(
        final String dbName,
        final String tblName,
        final Map<String, String> partVals,
        final PartitionEventType eventType
    ) throws TException {
        throw unimplemented("isPartitionMarkedForEvent", new Object[]{dbName, tblName, partVals, eventType});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<HiveObjectPrivilege> list_privileges(final String principalName, final PrincipalType principalType,
                                                     final HiveObjectRef hiveObject) throws TException {
        throw unimplemented("list_privileges", new Object[]{principalName, principalType, hiveObject});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Role> list_roles(final String principalName, final PrincipalType principalType) throws TException {
        throw unimplemented("list_roles", new Object[]{principalName, principalType});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LockResponse lock(final LockRequest rqst) throws TException {
        throw unimplemented("lock", new Object[]{rqst});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void markPartitionForEvent(final String dbName, final String tblName,
                                      final Map<String, String> partVals,
                                      final PartitionEventType eventType) throws TException {
        throw unimplemented("markPartitionForEvent", new Object[]{dbName, tblName, partVals, eventType});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OpenTxnsResponse open_txns(final OpenTxnRequest rqst) throws TException {
        throw unimplemented("open_txns", new Object[]{rqst});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean partition_name_has_valid_characters(final List<String> partVals, final boolean throwException)
        throws TException {
        return requestWrapper("partition_name_has_valid_characters", new Object[]{partVals, throwException},
            () -> {
                Pattern pattern = null;
                final String partitionPattern = config.getHivePartitionWhitelistPattern();
                if (!Strings.isNullOrEmpty(partitionPattern)) {
                    pattern = PATTERNS.getUnchecked(partitionPattern);
                }
                if (throwException) {
                    MetaStoreUtils.validatePartitionNameCharacters(partVals, pattern);
                    return true;
                } else {
                    return MetaStoreUtils.partitionNameHasValidCharacters(partVals, pattern);
                }
            });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public Map<String, String> partition_name_to_spec(final String partName) throws TException {
        return requestWrapper("partition_name_to_spec", new Object[]{partName}, () -> {
            if (Strings.isNullOrEmpty(partName)) {
                return (Map<String, String>) Collections.EMPTY_MAP;
            }

            return Warehouse.makeSpecFromName(partName);
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public List<String> partition_name_to_vals(final String partName) throws TException {
        return requestWrapper("partition_name_to_vals", new Object[]{partName}, () -> {
            if (Strings.isNullOrEmpty(partName)) {
                return (List<String>) Collections.EMPTY_LIST;
            }

            final Map<String, String> spec = Warehouse.makeSpecFromName(partName);
            final List<String> vals = Lists.newArrayListWithCapacity(spec.size());
            vals.addAll(spec.values());
            return vals;
        });
    }

    /**
     * Converts the partial specification given by the part_vals to a Metacat filter statement.
     *
     * @param dbName   the name of the database
     * @param tblName  the name of the table
     * @param partVals the partial specification values
     * @return A Meta
     * cat filter expression suitable for selecting out the partitions matching the specification
     * @throws MetaException if there are more part_vals specified than partition columns on the table.
     */
    @SuppressWarnings("checkstyle:methodname")
    String partition_values_to_partition_filter(final String dbName, final String tblName,
                                                final List<String> partVals)
        throws MetaException {
        final String databaseName = normalizeIdentifier(dbName);
        final String tableName = normalizeIdentifier(tblName);
        final TableDto tableDto = v1.getTable(catalogName, databaseName, tableName, true, false, false);
        return partition_values_to_partition_filter(tableDto, partVals);
    }

    /**
     * Converts the partial specification given by the part_vals to a Metacat filter statement.
     *
     * @param tableDto the Metacat representation of the table
     * @param partVals the partial specification values
     * @return A Metacat filter expression suitable for selecting out the partitions matching the specification
     * @throws MetaException if there are more part_vals specified than partition columns on the table.
     */
    @SuppressWarnings("checkstyle:methodname")
    String partition_values_to_partition_filter(final TableDto tableDto, final List<String> partVals)
        throws MetaException {
        if (partVals.size() > tableDto.getPartition_keys().size()) {
            throw new MetaException("Too many partition values for " + tableDto.getName());
        }

        final List<FieldDto> fields = tableDto.getFields();
        final List<String> partitionFilters = Lists.newArrayListWithCapacity(fields.size());
        for (int i = 0, partitionIdx = 0; i < fields.size(); i++) {
            final FieldDto fieldDto = fields.get(i);
            if (!fieldDto.isPartition_key() || partitionIdx >= partVals.size()) {
                continue;
            }

            final String partitionValueFilter = partVals.get(partitionIdx++);

            // We only filter on partitions with values
            if (!Strings.isNullOrEmpty(partitionValueFilter)) {
                String filter = "(" + fieldDto.getName() + "=";
                try {
                    filter += Long.parseLong(partitionValueFilter) + ")";
                } catch (NumberFormatException ignored) {
                    filter += "'" + partitionValueFilter + "')";
                }
                partitionFilters.add(filter);
            }
        }

        return partitionFilters.isEmpty() ? null : AND_JOINER.join(partitionFilters);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void rename_partition(final String dbName, final String tblName, final List<String> partVals,
                                 final Partition newPart)
        throws TException {
        requestWrapper("rename_partition", new Object[]{dbName, tblName, partVals}, () -> {
            final TableDto tableDto = getTableDto(dbName, tblName);
            final String partName = hiveConverters.getNameFromPartVals(tableDto, partVals);
            final PartitionsSaveRequestDto partitionsSaveRequestDto = new PartitionsSaveRequestDto();
            final PartitionDto partitionDto = hiveConverters.hiveToMetacatPartition(tableDto, newPart);
            partitionsSaveRequestDto.setPartitions(Lists.newArrayList(partitionDto));
            partitionsSaveRequestDto.setPartitionIdsForDeletes(Lists.newArrayList(partName));
            partV1.savePartitions(catalogName, dbName, tblName, partitionsSaveRequestDto);
            return null;
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long renew_delegation_token(final String tokenStrForm) throws TException {
        throw unimplemented("renew_delegation_token", new Object[]{tokenStrForm});
    }

    private TException unimplemented(final String methodName, final Object[] args) throws TException {
        log.info("+++ Thrift({}): Calling {}({})", catalogName, methodName, args);
        throw new InvalidOperationException("Not implemented yet: " + methodName);
    }

    private <R> R requestWrapper(final String methodName, final Object[] args, final ThriftSupplier<R> supplier)
        throws TException {
        final long start = registry.clock().wallTime();
        registry.counter(registry.createId(Metrics.CounterThrift.getMetricName() + "." + methodName)).increment();
        try {
            log.info("+++ Thrift({}): Calling {}({})", catalogName, methodName, args);
            return supplier.get();
        } catch (MetacatAlreadyExistsException e) {
            log.error(e.getMessage(), e);
            throw new AlreadyExistsException(e.getMessage());
        } catch (MetacatNotFoundException e) {
            log.error(e.getMessage(), e);
            throw new NoSuchObjectException(e.getMessage());
        } catch (MetacatPreconditionFailedException e) {
            log.error(e.getMessage(), e);
            throw new InvalidObjectException(e.getMessage());
        } catch (TException e) {
            log.error(e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            registry.counter(registry.createId(Metrics.CounterThrift.getMetricName() + "." + methodName)
                .withTags(Metrics.tagStatusFailureMap)).increment();
            final String message = String.format("%s -- %s failed", e.getMessage(), methodName);
            log.error(message, e);
            final MetaException me = new MetaException(message);
            me.initCause(e);
            throw me;
        } finally {
            final long duration = registry.clock().wallTime() - start;
            this.registry.timer(Metrics.TimerThriftRequest.getMetricName()
                + "." + methodName).record(duration, TimeUnit.MILLISECONDS);
            log.info("+++ Thrift({}): Time taken to complete {} is {} ms", catalogName, methodName, duration);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean revoke_privileges(final PrivilegeBag privileges) throws TException {
        throw unimplemented("revoke_privileges", new Object[]{privileges});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean revoke_role(final String roleName, final String principalName, final PrincipalType principalType)
        throws TException {
        throw unimplemented("revoke_role", new Object[]{roleName, principalName, principalType});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setMetaConf(final String key, final String value) throws TException {
        throw unimplemented("setMetaConf", new Object[]{key, value});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean set_aggr_stats_for(final SetPartitionsStatsRequest request) throws TException {
        throw unimplemented("set_aggr_stats_for", new Object[]{request});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> set_ugi(final String userName, final List<String> groupNames) throws TException {
        return requestWrapper("set_ugi", new Object[]{userName, groupNames}, () -> {
            Collections.addAll(groupNames, userName);
            return groupNames;
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ShowCompactResponse show_compact(final ShowCompactRequest rqst) throws TException {
        throw unimplemented("show_compact", new Object[]{rqst});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ShowLocksResponse show_locks(final ShowLocksRequest rqst) throws TException {
        throw unimplemented("show_locks", new Object[]{rqst});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unlock(final UnlockRequest rqst) throws TException {
        throw unimplemented("unlock", new Object[]{rqst});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean update_partition_column_statistics(final ColumnStatistics statsObj) throws TException {
        throw unimplemented("update_partition_column_statistics", new Object[]{statsObj});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean update_table_column_statistics(final ColumnStatistics statsObj) throws TException {
        throw unimplemented("update_table_column_statistics", new Object[]{statsObj});
    }

    @FunctionalInterface
    interface ThriftSupplier<T> {
        T get() throws TException;
    }
}
