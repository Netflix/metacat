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

package com.netflix.metacat.thrift;

import com.facebook.presto.hive.$internal.com.facebook.fb303.FacebookBase;
import com.facebook.presto.hive.$internal.com.facebook.fb303.FacebookService;
import com.facebook.presto.hive.$internal.com.facebook.fb303.fb_status;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.api.MetacatV1;
import com.netflix.metacat.common.api.PartitionV1;
import com.netflix.metacat.common.dto.DatabaseDto;
import com.netflix.metacat.common.dto.FieldDto;
import com.netflix.metacat.common.dto.GetPartitionsRequestDto;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.PartitionsSaveRequestDto;
import com.netflix.metacat.common.dto.StorageDto;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.exception.MetacatAlreadyExistsException;
import com.netflix.metacat.common.exception.MetacatNotFoundException;
import com.netflix.metacat.common.monitoring.CounterWrapper;
import com.netflix.metacat.common.monitoring.TimerWrapper;
import com.netflix.metacat.common.server.Config;
import com.netflix.metacat.common.util.MetacatContextManager;
import com.netflix.metacat.converters.HiveConverters;
import com.netflix.metacat.converters.TypeConverterProvider;
import com.netflix.metacat.thrift.CatalogThriftEventHandler.CatalogServerContext;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.PartitionExpressionProxy;
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
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionExpressionForMetastore;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

public class CatalogThriftHiveMetastore extends FacebookBase
        implements FacebookService.Iface, ThriftHiveMetastore.Iface {
    private static final Joiner AND_JOINER = Joiner.on(" and ");
    private static final LoadingCache<String, Pattern> PATTERNS = CacheBuilder.newBuilder()
            .build(new CacheLoader<String, Pattern>() {
                public Pattern load(@Nonnull String regex) {
                    return Pattern.compile(regex);
                }
            });
    private static final Logger log = LoggerFactory.getLogger(CatalogThriftHiveMetastore.class);
    private final String catalogName;
    private final Config config;
    private final HiveConverters hiveConverters;
    private final PartitionV1 partV1;
    private final TypeConverterProvider typeConverterProvider;
    private final MetacatV1 v1;
    private final PartitionExpressionProxy partitionExpressionProxy = new PartitionExpressionForMetastore();
    private final Map<String,List<PrivilegeGrantInfo>> defaultRolesPrivilegeSet =
            Maps.newHashMap(ImmutableMap.of("users",
                    Lists.newArrayList(new PrivilegeGrantInfo("ALL", 0, "hadoop", PrincipalType.ROLE, true))));

    public CatalogThriftHiveMetastore(Config config, TypeConverterProvider typeConverterProvider,
            HiveConverters hiveConverters, MetacatV1 metacatV1, PartitionV1 partitionV1,
            String catalogName) {
        super("CatalogThriftHiveMetastore");

        this.config = checkNotNull(config, "config is null");
        this.typeConverterProvider = checkNotNull(typeConverterProvider, "type converters is null");
        this.hiveConverters = checkNotNull(hiveConverters, "hive converters is null");
        this.v1 = checkNotNull(metacatV1, "metacat api is null");
        this.partV1 = checkNotNull(partitionV1, "partition api is null");
        this.catalogName = normalizeIdentifier(checkNotNull(catalogName, "catalog name is required"));
    }

    private static String normalizeIdentifier(String s) {
        if (s == null) {
            return null;
        } else {
            return s.trim().toLowerCase(Locale.ENGLISH);
        }
    }

    @Override
    public void abort_txn(AbortTxnRequest rqst) throws TException {
        throw unimplemented("abort_txn", new Object[] { rqst });
    }

    @Override
    public void add_dynamic_partitions(AddDynamicPartitions rqst) throws TException {
        throw unimplemented("add_dynamic_partitions", new Object[] { rqst });
    }

    @Override
    public Index add_index(Index new_index, Table index_table) throws TException {
        throw unimplemented("add_index", new Object[] { new_index, index_table });
    }

    @Override
    public Partition add_partition(Partition new_part) throws TException {
        return add_partition_with_environment_context(new_part, null);
    }

    @Override
    public Partition add_partition_with_environment_context(Partition new_part, EnvironmentContext ec)
            throws TException {
        String dbName = normalizeIdentifier(new_part.getDbName());
        String tableName = normalizeIdentifier(new_part.getTableName());
        return requestWrapper("add_partition_with_environment_context", new Object[] { dbName, tableName, ec }, () -> {
            addPartitionsCore(dbName, tableName, ImmutableList.of(new_part), false);
            return new_part;
        });
    }

    @Override
    public int add_partitions(List<Partition> new_parts) throws TException {
        if (new_parts == null || new_parts.size() == 0) {
            return 0;
        }
        String dbName = normalizeIdentifier(new_parts.get(0).getDbName());
        String tableName = normalizeIdentifier(new_parts.get(0).getTableName());
        return requestWrapper("add_partition", new Object[] { dbName, tableName }, () -> {
            addPartitionsCore(dbName, tableName, new_parts, false);
            return new_parts.size();
        });
    }

    @Override
    public int add_partitions_pspec(List<PartitionSpec> new_parts) throws TException {
        throw unimplemented("add_partitions_pspec", new Object[] { new_parts });
    }

    @Override
    public AddPartitionsResult add_partitions_req(AddPartitionsRequest request) throws TException {
        String dbName = normalizeIdentifier(request.getDbName());
        String tableName = normalizeIdentifier(request.getTblName());
        return requestWrapper("add_partition", new Object[] { dbName, tableName }, () -> {
            List<Partition> partitions = addPartitionsCore(dbName, tableName, request.getParts(),
                    request.isIfNotExists());
            AddPartitionsResult result = new AddPartitionsResult();
            result.setPartitions(partitions);
            return result;
        });
    }

    private List<Partition> addPartitionsCore(String dbName, String tblName, List<Partition> parts, boolean ifNotExists)
            throws TException {
        log.debug("Ignoring {} since metacat save partitions will do an update if it already exists", ifNotExists);
        TableDto tableDto = v1.getTable(catalogName, dbName, tblName, true, false, false);
        PartitionsSaveRequestDto partitionsSaveRequestDto = new PartitionsSaveRequestDto();
        List<PartitionDto> converted = Lists.newArrayListWithCapacity(parts.size());
        for (Partition partition : parts) {
            converted.add(hiveConverters.hiveToMetacatPartition(tableDto, partition));
        }
        partitionsSaveRequestDto.setPartitions(converted);
        partV1.savePartitions(catalogName, dbName, tblName, partitionsSaveRequestDto);
        return parts;
    }

    @Override
    public void alter_database(String dbname, Database db) throws TException {
        throw unimplemented("alter_database", new Object[] { dbname, db });
    }

    @Override
    public void alter_function(String dbName, String funcName, Function newFunc) throws TException {
        throw unimplemented("alter_function", new Object[] { dbName, funcName, newFunc });
    }

    @Override
    public void alter_index(String dbname, String base_tbl_name, String idx_name, Index new_idx) throws TException {
        throw unimplemented("alter_index", new Object[] { dbname, base_tbl_name, idx_name, new_idx });
    }

    @Override
    public void alter_partition(String db_name, String tbl_name, Partition new_part) throws TException {
        alter_partition_with_environment_context(db_name, tbl_name, new_part, null);
    }

    @Override
    public void alter_partition_with_environment_context(String db_name, String tbl_name, Partition new_part,
            EnvironmentContext ec) throws TException {
        String dbName = normalizeIdentifier(db_name);
        String tableName = normalizeIdentifier(tbl_name);
        requestWrapper("alter_partition_with_environment_context", new Object[] { dbName, tableName, ec }, () -> {
            addPartitionsCore(dbName, tableName, ImmutableList.of(new_part), false);
            return null;
        });
    }

    @Override
    public void alter_partitions(String db_name, String tbl_name, List<Partition> new_parts) throws TException {
        String dbName = normalizeIdentifier(db_name);
        String tableName = normalizeIdentifier(tbl_name);
        requestWrapper("add_partition", new Object[] { dbName, tableName }, () -> {
            addPartitionsCore(dbName, tableName, new_parts, false);
            return null;
        });
    }

    @Override
    public void alter_table(String _dbname, String _tbl_name, Table new_tbl) throws TException {
        alter_table_with_environment_context(_dbname, _tbl_name, new_tbl, null);
    }

    @Override
    public void alter_table_with_cascade(String dbname, String tbl_name, Table new_tbl, boolean cascade)
            throws TException {
        //TODO: Add logic to cascade the changes to the partitions
        alter_table_with_environment_context(dbname, tbl_name, new_tbl, null);
    }

    @Override
    public void alter_table_with_environment_context(String _dbname, String _tbl_name, Table new_tbl,
            EnvironmentContext environment_context) throws TException {
        requestWrapper("alter_table_with_environment_context",
                new Object[] { _dbname, _tbl_name, new_tbl, environment_context }, () -> {
                    String dbname = normalizeIdentifier(_dbname);
                    String tbl_name = normalizeIdentifier(_tbl_name);
                    QualifiedName oldName = QualifiedName.ofTable(catalogName, dbname, tbl_name);
                    QualifiedName newName = QualifiedName
                            .ofTable(catalogName, new_tbl.getDbName(), new_tbl.getTableName());

                    TableDto dto = hiveConverters.hiveToMetacatTable(newName, new_tbl);
                    if (!oldName.equals(newName)) {
                        v1.renameTable(catalogName, oldName.getDatabaseName(), oldName.getTableName(),
                                newName.getTableName());
                    }
                    v1.updateTable(catalogName, dbname, newName.getTableName(), dto);
                    return null;
                });
    }

    @Override
    public Partition append_partition(String db_name, String tbl_name, List<String> part_vals) throws TException {
        return append_partition_with_environment_context(db_name, tbl_name, part_vals, null);
    }

    @Override
    public Partition append_partition_by_name(String db_name, String tbl_name, String part_name) throws TException {
        return append_partition_by_name_with_environment_context(db_name, tbl_name, part_name, null);
    }

    @Override
    public Partition append_partition_by_name_with_environment_context(String db_name, String tbl_name,
            String part_name, EnvironmentContext environment_context) throws TException {
        return requestWrapper("append_partition_by_name_with_environment_context", new Object[] { db_name, tbl_name, part_name}, () -> appendPartitionsCoreAndReturn(db_name, tbl_name, part_name));
    }

    @Override
    public Partition append_partition_with_environment_context(String db_name, String tbl_name, List<String> part_vals,
            EnvironmentContext environment_context) throws TException {
        return requestWrapper("append_partition_by_name_with_environment_context", new Object[] { db_name, tbl_name, part_vals}, () -> {
            TableDto tableDto = getTableDto( db_name, tbl_name);
            String partName = hiveConverters.getNameFromPartVals(tableDto, part_vals);
            appendPartitionsCore(db_name, tbl_name, partName);
            return hiveConverters.metacatToHivePartition(getPartitionDtoByName(tableDto, partName), tableDto);
        });
    }

    private void appendPartitionsCore(String dbName, String tblName, String partName)
            throws TException {
        PartitionsSaveRequestDto partitionsSaveRequestDto = new PartitionsSaveRequestDto();
        PartitionDto partitionDto = new PartitionDto();
        partitionDto.setName(QualifiedName.ofPartition(catalogName, dbName, tblName, partName));
        partitionDto.setSerde(new StorageDto());
        partitionsSaveRequestDto.setPartitions(Lists.newArrayList(partitionDto));
        partV1.savePartitions(catalogName, dbName, tblName, partitionsSaveRequestDto);
    }

    private Partition appendPartitionsCoreAndReturn(String dbName, String tblName, String partName)
            throws TException {
        appendPartitionsCore(dbName, tblName, partName);
        return getPartitionByName(dbName, tblName, partName);
    }

    @Override
    public void cancel_delegation_token(String token_str_form) throws TException {
        throw unimplemented("cancel_delegation_token", new Object[] { token_str_form });
    }

    @Override
    public LockResponse check_lock(CheckLockRequest rqst) throws TException {
        throw unimplemented("check_lock", new Object[] { rqst });
    }

    @Override
    public void commit_txn(CommitTxnRequest rqst) throws TException {
        throw unimplemented("commit_txn", new Object[] { rqst });
    }

    @Override
    public void compact(CompactionRequest rqst) throws TException {
        throw unimplemented("compact", new Object[] { rqst });
    }

    @Override
    public void create_database(Database database) throws TException {
        requestWrapper("create_database", new Object[] { database }, () -> {
            String dbName = normalizeIdentifier(database.getName());
            v1.createDatabase(catalogName, dbName, null);
            return null;
        });
    }

    @Override
    public void create_function(Function func) throws TException {
        throw unimplemented("create_function", new Object[] { func });
    }

    @Override
    public boolean create_role(Role role) throws TException {
        throw unimplemented("create_role", new Object[] { role });
    }

    @Override
    public void create_table(Table tbl) throws TException {
        create_table_with_environment_context(tbl, null);
    }

    @Override
    public void create_table_with_environment_context(Table tbl, EnvironmentContext environment_context)
            throws TException {
        requestWrapper("create_table_with_environment_context", new Object[] { tbl, environment_context }, () -> {
            String dbname = normalizeIdentifier(tbl.getDbName());
            String tbl_name = normalizeIdentifier(tbl.getTableName());
            QualifiedName name = QualifiedName.ofTable(catalogName, dbname, tbl_name);

            TableDto dto = hiveConverters.hiveToMetacatTable(name, tbl);
            v1.createTable(catalogName, dbname, tbl_name, dto);
            return null;
        });
    }

    @Override
    public boolean create_type(Type type) throws TException {
        throw unimplemented("create_type", new Object[] { type });
    }

    @Override
    public boolean delete_partition_column_statistics(String db_name, String tbl_name, String part_name,
            String col_name) throws TException {
        throw unimplemented("delete_partition_column_statistics",
                new Object[] { db_name, tbl_name, part_name, col_name });
    }

    @Override
    public boolean delete_table_column_statistics(String db_name, String tbl_name, String col_name) throws TException {
        throw unimplemented("delete_table_column_statistics", new Object[] { db_name, tbl_name, col_name });
    }

    @Override
    public void drop_database(String name, boolean deleteData, boolean cascade) throws TException {
        throw unimplemented("drop_database", new Object[] { name, deleteData, cascade });
    }

    @Override
    public void drop_function(String dbName, String funcName) throws TException {
        throw unimplemented("drop_function", new Object[] { dbName, funcName });
    }

    @Override
    public boolean drop_index_by_name(String db_name, String tbl_name, String index_name, boolean deleteData)
            throws TException {
        throw unimplemented("drop_index_by_name", new Object[] { db_name, tbl_name, index_name, deleteData });
    }

    @Override
    public boolean drop_partition(String db_name, String tbl_name, List<String> part_vals, boolean deleteData)
            throws TException {
        return drop_partition_with_environment_context(db_name, tbl_name, part_vals, deleteData, null);
    }

    @Override
    public boolean drop_partition_by_name(String db_name, String tbl_name, String part_name, boolean deleteData)
            throws TException {
        return drop_partition_by_name_with_environment_context(db_name, tbl_name, part_name, deleteData, null);
    }

    @Override
    public boolean drop_partition_by_name_with_environment_context(String db_name, String tbl_name, String part_name,
            boolean deleteData, EnvironmentContext environment_context) throws TException {
        return requestWrapper("drop_partition_by_name_with_environment_context",
                new Object[] { db_name, tbl_name, part_name, deleteData, environment_context }, () -> {
                    String dbName = normalizeIdentifier(db_name);
                    String tableName = normalizeIdentifier(tbl_name);

                    if (deleteData) {
                        log.warn("Ignoring command to delete data for {}/{}/{}/{}",
                                catalogName, dbName, tableName, part_name);
                    }

                    partV1.deletePartitions(catalogName, dbName, tableName, ImmutableList.of(part_name));

                    return true;
                });
    }

    @Override
    public boolean drop_partition_with_environment_context(String db_name, String tbl_name, List<String> part_vals,
            boolean deleteData, EnvironmentContext environment_context) throws TException {
        return requestWrapper("drop_partition_with_environment_context",
                new Object[] { db_name, tbl_name, part_vals, deleteData, environment_context }, () -> {
                    TableDto tableDto = getTableDto(db_name, tbl_name);
                    String partName = hiveConverters.getNameFromPartVals(tableDto, part_vals);

                    QualifiedName partitionName = getPartitionDtoByName(tableDto, partName).getName();

                    if (deleteData) {
                        log.warn("Ignoring command to delete data for {}/{}/{}/{}", partitionName);
                    }

                    partV1.deletePartitions(
                            catalogName, tableDto.getName().getDatabaseName(), tableDto.getName().getTableName(), ImmutableList.of(partitionName.getPartitionName()));

                    return true;
                });
    }

    @Override
    public DropPartitionsResult drop_partitions_req(DropPartitionsRequest req) throws TException {
        throw unimplemented("drop_partitions_req", new Object[] { req });
    }

    @Override
    public boolean drop_role(String role_name) throws TException {
        throw unimplemented("drop_role", new Object[] { role_name });
    }

    @Override
    public void drop_table(String _dbname, String _name, boolean deleteData) throws TException {
        drop_table_with_environment_context(_dbname, _name, deleteData, null);
    }

    @Override
    public void drop_table_with_environment_context(String _dbname, String _name, boolean deleteData,
            EnvironmentContext ec) throws TException {
        requestWrapper("drop_table_with_environment_context", new Object[] { _dbname, _name, deleteData, ec }, () -> {
            String dbname = normalizeIdentifier(_dbname);
            String name = normalizeIdentifier(_name);

            if (deleteData) {
                log.warn("Ignoring command to delete data for {}/{}/{}", catalogName, dbname, name);
            }

            return v1.deleteTable(catalogName, dbname, name);
        });
    }

    @Override
    public boolean drop_type(String type) throws TException {
        throw unimplemented("drop_type", new Object[] { type });
    }

    @Override
    public Partition exchange_partition(Map<String, String> partitionSpecs, String source_db, String source_table_name,
            String dest_db, String dest_table_name) throws TException {
        throw unimplemented("exchange_partition",
                new Object[] { partitionSpecs, source_db, source_table_name, dest_db, dest_table_name });
    }

    @Override
    public FireEventResponse fire_listener_event(FireEventRequest rqst) throws TException {
        throw unimplemented("fire_listener_event", new Object[] { rqst });
    }

    @Override
    public String getCpuProfile(int i) throws TException {
        return "";
    }

    @Override
    public String getMetaConf(String key) throws TException {
        throw unimplemented("getMetaConf", new Object[] { key });
    }

    @Override
    public fb_status getStatus() {
        log.info("Thrift({}): Called getStatus", catalogName);
        return fb_status.ALIVE;
    }

    @Override
    public String getVersion() throws TException {
        log.info("Thrift({}): Called getVersion", catalogName);
        return "3.0";
    }

    @Override
    public AggrStats get_aggr_stats_for(PartitionsStatsRequest request) throws TException {
        throw unimplemented("get_aggr_stats_for", new Object[] { request });
    }

    @Override
    public List<String> get_all_databases() throws TException {
        return requestWrapper("get_all_databases", new Object[] {}, () -> v1.getCatalog(catalogName).getDatabases());
    }

    @Override
    public List<String> get_all_tables(String _db_name) throws TException {
        String db_name = normalizeIdentifier(_db_name);
        return requestWrapper("get_all_tables", new Object[] { _db_name },
                () -> v1.getDatabase(catalogName, db_name, false).getTables());
    }

    @Override
    public String get_config_value(String name, String defaultValue) throws TException {
        throw unimplemented("get_config_value", new Object[] { name, defaultValue });
    }

    @Override
    public CurrentNotificationEventId get_current_notificationEventId() throws TException {
        throw unimplemented("get_current_notificationEventId", new Object[] {});
    }

    @Override
    public Database get_database(String _name) throws TException {
        return requestWrapper("get_database", new Object[] { _name }, () -> {
            String name = normalizeIdentifier(_name);
            DatabaseDto dto = v1.getDatabase(catalogName, name, true);
            return hiveConverters.metacatToHiveDatabase(dto);
        });
    }

    @Override
    public List<String> get_databases(String hivePattern) throws TException {
        return requestWrapper("get_databases", new Object[] { hivePattern }, () -> {
            // Unsure about the pattern format.  I couldn't find any tests.  Assuming it is regex.
            Pattern pattern = PATTERNS.getUnchecked("(?i)" + hivePattern);
            List<String> databaseNames = v1.getCatalog(catalogName).getDatabases();
            return databaseNames.stream().filter(name -> pattern.matcher(name).matches()).collect(Collectors.toList());
        });
    }

    @Override
    public String get_delegation_token(String token_owner, String renewer_kerberos_principal_name) throws TException {
        throw unimplemented("get_delegation_token", new Object[] { token_owner, renewer_kerberos_principal_name });
    }

    @Override
    public List<FieldSchema> get_fields(String db_name, String table_name) throws TException {
        return get_fields_with_environment_context(db_name, table_name, null);
    }

    @Override
    public List<FieldSchema> get_fields_with_environment_context(String db_name, String table_name,
            EnvironmentContext environment_context) throws TException {
        return requestWrapper("get_fields_with_environment_context",
                new Object[] { db_name, table_name, environment_context }, () -> {
                    Table table = get_table(db_name, table_name);

                    if (table == null || table.getSd() == null || table.getSd().getCols() == null) {
                        throw new MetaException("Unable to get fields for " + db_name + "." + table_name);
                    }
                    return table.getSd().getCols();
                });
    }

    @Override
    public Function get_function(String dbName, String funcName) throws TException {
        throw unimplemented("get_function", new Object[] { dbName, funcName });
    }

    @Override
    public List<String> get_functions(String dbName, String pattern) throws TException {
        throw unimplemented("get_functions", new Object[] { dbName, pattern });
    }

    @Override
    public Index get_index_by_name(String db_name, String tbl_name, String index_name) throws TException {
        throw unimplemented("get_index_by_name", new Object[] { db_name, tbl_name, index_name });
    }

    @Override
    public List<String> get_index_names(String db_name, String tbl_name, short max_indexes) throws TException {
        throw unimplemented("get_index_names", new Object[] { db_name, tbl_name, max_indexes });
    }

    @Override
    public List<Index> get_indexes(String db_name, String tbl_name, short max_indexes) throws TException {
        throw unimplemented("get_indexes", new Object[] { db_name, tbl_name, max_indexes });
    }

    @Override
    public NotificationEventResponse get_next_notification(NotificationEventRequest rqst) throws TException {
        throw unimplemented("get_next_notification", new Object[] { rqst });
    }

    @Override
    public GetOpenTxnsResponse get_open_txns() throws TException {
        throw unimplemented("get_open_txns", new Object[] {});
    }

    @Override
    public GetOpenTxnsInfoResponse get_open_txns_info() throws TException {
        throw unimplemented("get_open_txns_info", new Object[] {});
    }

    @Override
    public List<PartitionSpec> get_part_specs_by_filter(String db_name, String tbl_name, String filter, int max_parts)
            throws TException {
        throw unimplemented("get_part_specs_by_filter", new Object[] { db_name, tbl_name, filter, max_parts });
    }

    @Override
    public Partition get_partition(String _db_name, String _tbl_name, List<String> part_vals) throws TException {
        return requestWrapper("get_partition", new Object[] { _db_name, _tbl_name, part_vals }, () -> {
            TableDto tableDto = getTableDto( _db_name, _tbl_name);
            String partName = hiveConverters.getNameFromPartVals(tableDto, part_vals);
            return hiveConverters.metacatToHivePartition(getPartitionDtoByName(tableDto, partName), tableDto);
        });
    }

    @Override
    public Partition get_partition_by_name(String _db_name, String _tbl_name, String part_name) throws TException {
        return requestWrapper("get_partition_by_name", new Object[] { _db_name, _tbl_name, part_name }, () -> getPartitionByName(_db_name, _tbl_name, part_name));
    }

    private TableDto getTableDto(String _db_name, String _tbl_name){
        String db_name = normalizeIdentifier(_db_name);
        String tbl_name = normalizeIdentifier(_tbl_name);
        return v1.getTable(catalogName, db_name, tbl_name, true, false, false);
    }

    private Partition getPartitionByName(String _db_name, String _tbl_name, String part_name) throws TException{
        TableDto tableDto = getTableDto( _db_name, _tbl_name);
        return hiveConverters.metacatToHivePartition(getPartitionDtoByName(tableDto, part_name), tableDto);
    }

    private PartitionDto getPartitionDtoByName(TableDto tableDto, String part_name) throws TException{
        GetPartitionsRequestDto dto = new GetPartitionsRequestDto();
        dto.setIncludePartitionDetails(true);
        dto.setPartitionNames(ImmutableList.of(part_name));
        List<PartitionDto> partitionDtos = partV1.getPartitionsForRequest(
                catalogName, tableDto.getName().getDatabaseName(), tableDto.getName().getTableName(), null, null, null, null, false, dto);

        if (partitionDtos == null || partitionDtos.isEmpty()) {
            throw new NoSuchObjectException("Partition (" + part_name + ") not found on " + tableDto.getName());
        } else if(partitionDtos.size() != 1) {
            // I don't think this is even possible
            throw new NoSuchObjectException("Partition (" + part_name + ") matched extra on " + tableDto.getName());
        }
        return partitionDtos.get(0);
    }

    @Override
    public ColumnStatistics get_partition_column_statistics(String db_name, String tbl_name, String part_name,
            String col_name) throws TException {
        throw unimplemented("get_partition_column_statistics", new Object[] { db_name, tbl_name, part_name, col_name });
    }

    @Override
    public List<String> get_partition_names(String db_name, String tbl_name, short max_parts) throws TException {
        String dbName = normalizeIdentifier(db_name);
        String tableName = normalizeIdentifier(tbl_name);
        Integer maxValues = max_parts > 0 ? Short.toUnsignedInt(max_parts) : null;
        return requestWrapper("get_partition_names", new Object[] { dbName, tableName, max_parts }, () -> partV1
                .getPartitionKeys(catalogName, dbName, tableName, null, null, null, null, maxValues));
    }

    @Override
    public List<String> get_partition_names_ps(String _db_name, String _tbl_name, List<String> part_vals,
            short max_parts) throws TException {
        return requestWrapper("get_partition_names_ps", new Object[] { _db_name, _tbl_name, part_vals, max_parts },
                () -> {
                    String db_name = normalizeIdentifier(_db_name);
                    String tbl_name = normalizeIdentifier(_tbl_name);
                    String partFilter = partition_values_to_partition_filter(db_name, tbl_name, part_vals);

                    Integer maxValues = max_parts > 0 ? Short.toUnsignedInt(max_parts) : null;
                    return partV1.getPartitionKeys(catalogName, db_name, tbl_name, partFilter, null, null, null, maxValues);
                });
    }

    @Override
    public Partition get_partition_with_auth(String db_name, String tbl_name, List<String> part_vals, String user_name,
            List<String> group_names) throws TException {
        //TODO: Handle setting the privileges
        return get_partition( db_name, tbl_name, part_vals);
    }

    @Override
    public List<Partition> get_partitions(String db_name, String tbl_name, short max_parts) throws TException {
        return requestWrapper("get_partitions", new Object[] { db_name, tbl_name, max_parts }, () -> {
            String dbName = normalizeIdentifier(db_name);
            String tableName = normalizeIdentifier(tbl_name);
            TableDto tableDto = v1.getTable(catalogName, dbName, tableName, true, false, false);

            Integer maxValues = max_parts > 0 ? Short.toUnsignedInt(max_parts) : null;
            List<PartitionDto> metacatPartitions = partV1.getPartitions(catalogName, db_name, tbl_name, null, null,
                    null, null, maxValues, false);
            List<Partition> result = Lists.newArrayListWithCapacity(metacatPartitions.size());
            for (PartitionDto partition : metacatPartitions) {
                result.add(hiveConverters.metacatToHivePartition(partition, tableDto));
            }
            return result;
        });
    }

    @Override
    public PartitionsByExprResult get_partitions_by_expr(PartitionsByExprRequest req) throws TException {
        throw unimplemented("get_partitions_by_expr", new Object[] { req });
    }

    @Override
    public List<Partition> get_partitions_by_filter(String db_name, String tbl_name, String filter, short max_parts)
            throws TException {
        return requestWrapper("get_partitions_by_filter", new Object[] { db_name, tbl_name, filter, max_parts }, () -> {
            String dbName = normalizeIdentifier(db_name);
            String tableName = normalizeIdentifier(tbl_name);
            TableDto tableDto = v1.getTable(catalogName, dbName, tableName, true, false, false);

            Integer maxValues = max_parts > 0 ? Short.toUnsignedInt(max_parts) : null;
            List<PartitionDto> metacatPartitions = partV1.getPartitions(catalogName, db_name, tbl_name, filter, null,
                    null, null, maxValues, false);
            List<Partition> result = Lists.newArrayListWithCapacity(metacatPartitions.size());
            for (PartitionDto partition : metacatPartitions) {
                result.add(hiveConverters.metacatToHivePartition(partition, tableDto));
            }
            return result;
        });
    }

    @Override
    public List<Partition> get_partitions_by_names(String _db_name, String _tbl_name, List<String> names)
            throws TException {
        return requestWrapper("get_partitions_by_names", new Object[] { _db_name, _tbl_name, names }, () -> {
            String db_name = normalizeIdentifier(_db_name);
            String tbl_name = normalizeIdentifier(_tbl_name);
            TableDto tableDto = v1.getTable(catalogName, db_name, tbl_name, true, false, false);

            GetPartitionsRequestDto dto = new GetPartitionsRequestDto();
            dto.setIncludePartitionDetails(true);
            dto.setPartitionNames(names);
            List<PartitionDto> metacatPartitions = partV1.getPartitionsForRequest(catalogName, db_name, tbl_name, null,
                    null, null, null, false, dto);
            List<Partition> result = Lists.newArrayListWithCapacity(metacatPartitions.size());
            for (PartitionDto partition : metacatPartitions) {
                result.add(hiveConverters.metacatToHivePartition(partition, tableDto));
            }
            return result;
        });
    }

    @Override
    public List<Partition> get_partitions_ps(String db_name, String tbl_name, List<String> part_vals, short max_parts)
            throws TException {
        return requestWrapper("get_partitions_ps", new Object[] { db_name, tbl_name, part_vals, max_parts }, () -> {
            String dbName = normalizeIdentifier(db_name);
            String tableName = normalizeIdentifier(tbl_name);
            TableDto tableDto = v1.getTable(catalogName, dbName, tableName, true, false, false);
            String partFilter = partition_values_to_partition_filter(tableDto, part_vals);

            Integer maxValues = max_parts > 0 ? Short.toUnsignedInt(max_parts) : null;
            List<PartitionDto> metacatPartitions = partV1.getPartitions(catalogName, db_name, tbl_name, partFilter,
                    null, null, null, maxValues, false);
            List<Partition> result = Lists.newArrayListWithCapacity(metacatPartitions.size());
            for (PartitionDto partition : metacatPartitions) {
                result.add(hiveConverters.metacatToHivePartition(partition, tableDto));
            }
            return result;
        });
    }

    @Override
    public List<Partition> get_partitions_ps_with_auth(String db_name, String tbl_name, List<String> part_vals,
            short max_parts, String user_name, List<String> group_names) throws TException {
        //TODO: Handle setting the privileges
        return get_partitions_ps(db_name, tbl_name, part_vals, max_parts);
    }

    @Override
    public List<PartitionSpec> get_partitions_pspec(String db_name, String tbl_name, int max_parts) throws TException {
        throw unimplemented("get_partitions_pspec", new Object[] { db_name, tbl_name, max_parts });
    }

    @Override
    public PartitionsStatsResult get_partitions_statistics_req(PartitionsStatsRequest request) throws TException {
        throw unimplemented("get_partitions_statistics_req", new Object[] { request });
    }

    @Override
    public List<Partition> get_partitions_with_auth(String db_name, String tbl_name, short max_parts, String user_name,
            List<String> group_names) throws TException {
        //TODO: Handle setting the privileges
        return get_partitions(db_name, tbl_name, max_parts);
    }

    @Override
    public GetPrincipalsInRoleResponse get_principals_in_role(GetPrincipalsInRoleRequest request) throws TException {
        throw unimplemented("get_principals_in_role", new Object[] { request });
    }

    @Override
    public PrincipalPrivilegeSet get_privilege_set(HiveObjectRef hiveObject, String userName, List<String> groupNames)
            throws TException {
        return requestWrapper("get_privilege_set", new Object[] { hiveObject, userName, groupNames },
                () -> {
                    Map<String,List<PrivilegeGrantInfo>> groupPrivilegeSet = null;
                    Map<String,List<PrivilegeGrantInfo>> userPrivilegeSet = null;

                    if( groupNames != null) {
                        groupPrivilegeSet = groupNames.stream()
                                .collect(Collectors.toMap(p -> p, p -> Lists.newArrayList()));
                    }
                    if( userName != null){
                        userPrivilegeSet = ImmutableMap.of(userName, Lists.newArrayList());
                    }
                    return new PrincipalPrivilegeSet( userPrivilegeSet,
                                groupPrivilegeSet,
                                defaultRolesPrivilegeSet);
                });
    }

    @Override
    public GetRoleGrantsForPrincipalResponse get_role_grants_for_principal(GetRoleGrantsForPrincipalRequest request)
            throws TException {
        throw unimplemented("get_role_grants_for_principal", new Object[] { request });
    }

    @Override
    public List<String> get_role_names() throws TException {
        throw unimplemented("get_role_names", new Object[] {});
    }

    @Override
    public List<FieldSchema> get_schema(String db_name, String table_name) throws TException {
        return get_schema_with_environment_context(db_name, table_name, null);
    }

    @Override
    public List<FieldSchema> get_schema_with_environment_context(String db_name, String table_name,
            EnvironmentContext environment_context) throws TException {
        return requestWrapper("get_schema_with_environment_context",
                new Object[] { db_name, table_name, environment_context }, () -> {
                    Table table = get_table(db_name, table_name);
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
                                "Table does not have any partition keys or cols: " + db_name + "." + table_name);
                    }

                    List<FieldSchema> result = Lists.newArrayListWithCapacity(partitionKeys.size() + columns.size());
                    result.addAll(columns);
                    result.addAll(partitionKeys);
                    return result;
                });
    }

    @Override
    public Table get_table(String _dbname, String _tbl_name) throws TException {
        return requestWrapper("get_table", new Object[] { _dbname, _tbl_name }, () -> {
            String dbname = normalizeIdentifier(_dbname);
            String tbl_name = normalizeIdentifier(_tbl_name);

            TableDto dto = v1.getTable(catalogName, dbname, tbl_name, true, true, true);
            return hiveConverters.metacatToHiveTable(dto);
        });
    }

    @Override
    public ColumnStatistics get_table_column_statistics(String db_name, String tbl_name, String col_name)
            throws TException {
        throw unimplemented("get_table_column_statistics", new Object[] { db_name, tbl_name, col_name });
    }

    @Override
    public List<String> get_table_names_by_filter(String _dbname, String filter, short max_tables) throws TException {
        return requestWrapper("get_table_names_by_filter", new Object[] { _dbname, filter, max_tables }, () -> {
            String dbname = normalizeIdentifier(_dbname);

            List<String> tables = v1.getDatabase(catalogName, dbname, false).getTables();
            // TODO apply filter
            if (!"hive_filter_field_params__presto_view = \"true\"".equals(filter)) {
                throw new IllegalArgumentException("Unexpected filter: '" + filter + "'");
            }
            if (max_tables <= 0 || max_tables >= tables.size()) {
                return tables;
            } else {
                return tables.subList(0, max_tables);
            }
        });
    }

    @Override
    public List<Table> get_table_objects_by_name(String _dbname, List<String> tbl_names) throws TException {
        return requestWrapper("get_table_objects_by_name", new Object[] { _dbname, tbl_names }, () -> {
            String dbname = normalizeIdentifier(_dbname);
            return tbl_names.stream()
                            .map(CatalogThriftHiveMetastore::normalizeIdentifier)
                            .map(name -> v1.getTable(catalogName, dbname, name, true, true, true))
                            .map(tableDto -> hiveConverters.metacatToHiveTable(tableDto))
                            .collect(Collectors.toList());
        });
    }

    @Override
    public TableStatsResult get_table_statistics_req(TableStatsRequest request) throws TException {
        throw unimplemented("get_table_statistics_req", new Object[] { request });
    }

    @Override
    public List<String> get_tables(String db_name, String hivePattern) throws TException {
        return requestWrapper("get_tables", new Object[] { db_name, hivePattern }, () -> {
            // Unsure about the pattern format.  I couldn't find any tests.  Assuming it is regex.
            Pattern pattern = PATTERNS.getUnchecked("(?i)" + hivePattern);
            List<String> tableNames = v1.getDatabase(catalogName, db_name, false).getTables();
            return tableNames.stream().filter(name -> pattern.matcher(name).matches()).collect(Collectors.toList());
        });
    }

    @Override
    public Type get_type(String name) throws TException {
        throw unimplemented("get_type", new Object[] { name });
    }

    @Override
    public Map<String, Type> get_type_all(String name) throws TException {
        throw unimplemented("get_type_all", new Object[] { name });
    }

    @Override
    public boolean grant_privileges(PrivilegeBag privileges) throws TException {
        throw unimplemented("grant_privileges", new Object[] { privileges });
    }

    @Override
    public GrantRevokePrivilegeResponse grant_revoke_privileges(GrantRevokePrivilegeRequest request) throws TException {
        throw unimplemented("grant_revoke_privileges", new Object[] { request });
    }

    @Override
    public GrantRevokeRoleResponse grant_revoke_role(GrantRevokeRoleRequest request) throws TException {
        throw unimplemented("grant_revoke_role", new Object[] { request });
    }

    @Override
    public boolean grant_role(String role_name, String principal_name, PrincipalType principal_type, String grantor,
            PrincipalType grantorType, boolean grant_option) throws TException {
        throw unimplemented("grant_role",
                new Object[] { role_name, principal_name, principal_type, grantor, grantorType });
    }

    @Override
    public void heartbeat(HeartbeatRequest ids) throws TException {
        throw unimplemented("heartbeat", new Object[] { ids });
    }

    @Override
    public HeartbeatTxnRangeResponse heartbeat_txn_range(HeartbeatTxnRangeRequest txns) throws TException {
        throw unimplemented("heartbeat_txn_range", new Object[] { txns });
    }

    @Override
    public boolean isPartitionMarkedForEvent(String db_name, String tbl_name, Map<String, String> part_vals,
            PartitionEventType eventType) throws TException {
        throw unimplemented("isPartitionMarkedForEvent", new Object[] { db_name, tbl_name, part_vals, eventType });
    }

    @Override
    public List<HiveObjectPrivilege> list_privileges(String principal_name, PrincipalType principal_type,
            HiveObjectRef hiveObject) throws TException {
        throw unimplemented("list_privileges", new Object[] { principal_name, principal_type, hiveObject });
    }

    @Override
    public List<Role> list_roles(String principal_name, PrincipalType principal_type) throws TException {
        throw unimplemented("list_roles", new Object[] { principal_name, principal_type });
    }

    @Override
    public LockResponse lock(LockRequest rqst) throws TException {
        throw unimplemented("lock", new Object[] { rqst });
    }

    @Override
    public void markPartitionForEvent(String db_name, String tbl_name, Map<String, String> part_vals,
            PartitionEventType eventType) throws TException {
        throw unimplemented("markPartitionForEvent", new Object[] { db_name, tbl_name, part_vals, eventType });
    }

    @Override
    public OpenTxnsResponse open_txns(OpenTxnRequest rqst) throws TException {
        throw unimplemented("open_txns", new Object[] { rqst });
    }

    @Override
    public boolean partition_name_has_valid_characters(List<String> part_vals, boolean throw_exception)
            throws TException {
        return requestWrapper("partition_name_has_valid_characters", new Object[] { part_vals, throw_exception },
                () -> {
                    Pattern pattern = null;
                    String partitionPattern = config.getHivePartitionWhitelistPattern();
                    if (!Strings.isNullOrEmpty(partitionPattern)) {
                        pattern = PATTERNS.getUnchecked(partitionPattern);
                    }
                    if (throw_exception) {
                        MetaStoreUtils.validatePartitionNameCharacters(part_vals, pattern);
                        return true;
                    } else {
                        return MetaStoreUtils.partitionNameHasValidCharacters(part_vals, pattern);
                    }
                });
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, String> partition_name_to_spec(String part_name) throws TException {
        return requestWrapper("partition_name_to_spec", new Object[] { part_name }, () -> {
            if (Strings.isNullOrEmpty(part_name)) {
                return (Map<String, String>) Collections.EMPTY_MAP;
            }

            return Warehouse.makeSpecFromName(part_name);
        });
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<String> partition_name_to_vals(String part_name) throws TException {
        return requestWrapper("partition_name_to_vals", new Object[] { part_name }, () -> {
            if (Strings.isNullOrEmpty(part_name)) {
                return (List<String>) Collections.EMPTY_LIST;
            }

            Map<String, String> spec = Warehouse.makeSpecFromName(part_name);
            List<String> vals = Lists.newArrayListWithCapacity(spec.size());
            vals.addAll(spec.values());
            return vals;
        });
    }

    /**
     * Converts the partial specification given by the part_vals to a Metacat filter statement.
     * @param dbName the name of the database
     * @param tableName the name of the table
     * @param part_vals the partial specification values
     * @return A Metacat filter expression suitable for selecting out the partitions matching the specification
     * @throws MetaException if there are more part_vals specified than partition columns on the table.
     */
    String partition_values_to_partition_filter(String dbName, String tableName, List<String> part_vals)
            throws MetaException {
        dbName = normalizeIdentifier(dbName);
        tableName = normalizeIdentifier(tableName);
        TableDto tableDto = v1.getTable(catalogName, dbName, tableName, true, false, false);
        return partition_values_to_partition_filter(tableDto, part_vals);
    }

    /**
     * Converts the partial specification given by the part_vals to a Metacat filter statement.
     * @param tableDto the Metacat representation of the table
     * @param part_vals the partial specification values
     * @return A Metacat filter expression suitable for selecting out the partitions matching the specification
     * @throws MetaException if there are more part_vals specified than partition columns on the table.
     */
    String partition_values_to_partition_filter(TableDto tableDto, List<String> part_vals)
            throws MetaException {
        if (part_vals.size() > tableDto.getPartition_keys().size()) {
            throw new MetaException("Too many partition values for " + tableDto.getName());
        }

        List<FieldDto> fields = tableDto.getFields();
        List<String> partitionFilters = Lists.newArrayListWithCapacity(fields.size());
        for (int i = 0, partitionIdx = 0; i < fields.size(); i++) {
            FieldDto f_dto = fields.get(i);
            if (!f_dto.isPartition_key() || partitionIdx >= part_vals.size()) {
                continue;
            }

            String partitionValueFilter = part_vals.get(partitionIdx++);

            // We only filter on partitions with values
            if (!Strings.isNullOrEmpty(partitionValueFilter)) {
                String filter = "(" + f_dto.getName() + "=";
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

    @Override
    public void rename_partition(String db_name, String tbl_name, List<String> part_vals, Partition new_part)
            throws TException {
        requestWrapper("rename_partition", new Object[] { db_name, tbl_name, part_vals }, () -> {
            TableDto tableDto = getTableDto( db_name, tbl_name);
            String partName = hiveConverters.getNameFromPartVals(tableDto, part_vals);
            PartitionsSaveRequestDto partitionsSaveRequestDto = new PartitionsSaveRequestDto();
            PartitionDto partitionDto = hiveConverters.hiveToMetacatPartition(tableDto, new_part);
            partitionsSaveRequestDto.setPartitions(Lists.newArrayList(partitionDto));
            partitionsSaveRequestDto.setPartitionIdsForDeletes(Lists.newArrayList(partName));
            partV1.savePartitions(catalogName, db_name, tbl_name, partitionsSaveRequestDto);
            return null;
        });
    }

    @Override
    public long renew_delegation_token(String token_str_form) throws TException {
        throw unimplemented("renew_delegation_token", new Object[] { token_str_form });
    }

    TException unimplemented(String methodName, Object[] args) throws TException {
        log.info("+++ Thrift({}): Calling {}({})", catalogName, methodName, args);
        throw new InvalidOperationException("Not implemented yet: " + methodName);
    }

    <R> R requestWrapper(String methodName, Object[] args, ThriftSupplier<R> supplier) throws TException {
        TimerWrapper timer = TimerWrapper.createStarted("dse.metacat.thrift.timer." + methodName);
        CounterWrapper.incrementCounter("dse.metacat.thrift.counter." + methodName);
        try {
            log.info("+++ Thrift({}): Calling {}({})", catalogName, methodName, args);
            return supplier.get();
        } catch (MetacatAlreadyExistsException e) {
            log.error(e.getMessage(), e);
            throw new AlreadyExistsException(e.getMessage());
        } catch (InvalidOperationException e) {
            log.error(e.getMessage(), e);
            throw e;
        } catch (MetacatNotFoundException e) {
            log.error(e.getMessage(), e);
            throw new NoSuchObjectException(e.getMessage());
        } catch (Exception e) {
            CounterWrapper.incrementCounter("dse.metacat.thrift.counter.failure." + methodName);
            String message = String.format("%s -- %s failed", e.getMessage(), methodName);
            log.error(message, e);
            MetaException me = new MetaException(message);
            me.initCause(e);
            throw e;
        } finally {
            log.info("+++ Thrift({}): Time taken to complete {} is {} ms", catalogName, methodName, timer.stop());
        }
    }

    private void revertToDefaultTypeConverter() {
        // Switch back to the original/default type converter
        CatalogServerContext orig = (CatalogServerContext) MetacatContextManager.getContext();
        CatalogServerContext context = new CatalogServerContext(orig.getUserName(), orig.getClientAppName(),
                orig.getClientId(), orig.getJobId(), typeConverterProvider.getDefaultConverterType().name(),
                orig.requestThreadId);

        MetacatContextManager.setContext(context);
    }

    @Override
    public boolean revoke_privileges(PrivilegeBag privileges) throws TException {
        throw unimplemented("revoke_privileges", new Object[] { privileges });
    }

    @Override
    public boolean revoke_role(String role_name, String principal_name, PrincipalType principal_type)
            throws TException {
        throw unimplemented("revoke_role", new Object[] { role_name, principal_name, principal_type });
    }

    @Override
    public void setMetaConf(String key, String value) throws TException {
        throw unimplemented("setMetaConf", new Object[] { key, value });
    }

    @Override
    public boolean set_aggr_stats_for(SetPartitionsStatsRequest request) throws TException {
        throw unimplemented("set_aggr_stats_for", new Object[] { request });
    }

    @Override
    public List<String> set_ugi(String user_name, List<String> group_names) throws TException {
        return requestWrapper("set_ugi", new Object[] { user_name, group_names }, () -> {
            Collections.addAll(group_names, user_name);
            return group_names;
        });
    }

    @Override
    public ShowCompactResponse show_compact(ShowCompactRequest rqst) throws TException {
        throw unimplemented("show_compact", new Object[] { rqst });
    }

    @Override
    public ShowLocksResponse show_locks(ShowLocksRequest rqst) throws TException {
        throw unimplemented("show_locks", new Object[] { rqst });
    }

    @Override
    public void unlock(UnlockRequest rqst) throws TException {
        throw unimplemented("unlock", new Object[] { rqst });
    }

    @Override
    public boolean update_partition_column_statistics(ColumnStatistics stats_obj) throws TException {
        throw unimplemented("update_partition_column_statistics", new Object[] { stats_obj });
    }

    @Override
    public boolean update_table_column_statistics(ColumnStatistics stats_obj) throws TException {
        throw unimplemented("update_table_column_statistics", new Object[] { stats_obj });
    }

    @FunctionalInterface
    interface ThriftSupplier<T> {
        T get() throws TException;
    }
}
