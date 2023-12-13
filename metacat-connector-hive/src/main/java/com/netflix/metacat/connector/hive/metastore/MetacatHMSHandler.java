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
package com.netflix.metacat.connector.hive.metastore;

import com.netflix.metacat.connector.hive.monitoring.HiveMetrics;
import com.netflix.spectator.api.NoopRegistry;
import com.netflix.spectator.api.Registry;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.metrics.Metrics;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.MetaStoreEndFunctionContext;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.RawStoreProxy;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * This is an extension of the HiveMetastore. This provides multi-tenancy to the hive metastore.
 *
 * @author amajumdar
 * @since 1.0.0
 */
public class MetacatHMSHandler extends HiveMetaStore.HMSHandler implements IMetacatHMSHandler {
    private Pattern partitionValidationPattern;
    private int nextSerialNum;
    private final Registry registry;

    private ThreadLocal<Integer> threadLocalId = new ThreadLocal<Integer>() {
        @Override
        protected synchronized Integer initialValue() {
            return nextSerialNum++;
        }
    };

    private final ThreadLocal<RawStore> threadLocalMS =
            new ThreadLocal<RawStore>() {
                @Override
                protected synchronized RawStore initialValue() {
                    return null;
                }
            };

    private final ThreadLocal<Configuration> threadLocalConf =
            new ThreadLocal<Configuration>() {
                @Override
                protected synchronized Configuration initialValue() {
                    return null;
                }
            };

    /**
     * Constructor.
     *
     * @param name client name
     * @throws MetaException exception
     */
    public MetacatHMSHandler(final String name) throws MetaException {
        this(name, new HiveConf(HiveMetaStore.HMSHandler.class));
    }

    /**
     * Constructor.
     *
     * @param name client name
     * @param conf hive configurations
     * @throws MetaException exception
     */
    public MetacatHMSHandler(final String name, final HiveConf conf) throws MetaException {
        this(name, conf, new NoopRegistry(), true);
    }

    /**
     * Constructor.
     *
     * @param name client name
     * @param conf hive configurations
     * @param registry registry
     * @param init initialize if true.
     * @throws MetaException exception
     */
    public MetacatHMSHandler(final String name, final HiveConf conf, final Registry registry, final boolean init)
        throws MetaException {
        super(name, conf, init);
        this.registry = registry;
        final String partitionValidationRegex =
            getHiveConf().getVar(HiveConf.ConfVars.METASTORE_PARTITION_NAME_WHITELIST_PATTERN);
        if (partitionValidationRegex != null && !partitionValidationRegex.isEmpty()) {
            partitionValidationPattern = Pattern.compile(partitionValidationRegex);
        } else {
            partitionValidationPattern = null;
        }
    }

    @Override
    public RawStore getMS() throws MetaException {
        RawStore ms = threadLocalMS.get();
        if (ms == null) {
            ms = newRawStore();
            ms.verifySchema();
            threadLocalMS.set(ms);
            ms = threadLocalMS.get();
        }
        return ms;
    }

    @Override
    public void setConf(final Configuration conf) {
        threadLocalConf.set(conf);
        final RawStore ms = threadLocalMS.get();
        if (ms != null) {
            ms.setConf(conf); // reload if DS related configuration is changed
        }
    }

    @Override
    public Configuration getConf() {
        Configuration conf = threadLocalConf.get();
        if (conf == null) {
            conf = new Configuration(getHiveConf());
            threadLocalConf.set(conf);
        }
        return conf;
    }

    private RawStore newRawStore() throws MetaException {
        final Configuration conf = getConf();
        final String rawStoreClassName = getHiveConf().getVar(HiveConf.ConfVars.METASTORE_RAW_STORE_IMPL);
        LOG.info(String.format("%s: Opening raw store with implemenation class: %s", threadLocalId.get(),
                rawStoreClassName));
        return RawStoreProxy.getProxy(getHiveConf(), conf, rawStoreClassName, threadLocalId.get());
    }

    private void logInfo(final String m) {
        LOG.info(threadLocalId.get().toString() + ": " + m);
    }

    private String startFunction(final String function, final String extraLogInfo) {
        incrementCounter(function);
        logInfo((getIpAddress() == null ? "" : "source:" + getIpAddress() + " ") + function + extraLogInfo);
        try {
            Metrics.startScope(function);
        } catch (IOException e) {
            LOG.debug("Exception when starting metrics scope"
                    + e.getClass().getName() + " " + e.getMessage(), e);
        }
        return function;
    }

    private String startFunction(final String function) {
        return startFunction(function, "");
    }

    private void endFunction(final String function, final boolean successful, final Exception e,
                             final String inputTableName) {
        endFunction(function, new MetaStoreEndFunctionContext(successful, e, inputTableName));
    }

    private void endFunction(final String function, final MetaStoreEndFunctionContext context) {
        try {
            Metrics.endScope(function);
        } catch (IOException e) {
            LOG.debug("Exception when closing metrics scope" + e);
        }
    }

    private static MetaException newMetaException(final Exception e) {
        final MetaException me = new MetaException(e.toString());
        me.initCause(e);
        return me;
    }

    private static class PartValEqWrapper {
        private Partition partition;

        /**
         * Constructor.
         *
         * @param partition partition
         */
        PartValEqWrapper(final Partition partition) {
            this.partition = partition;
        }

        @Override
        public int hashCode() {
            return partition.isSetValues() ? partition.getValues().hashCode() : 0;
        }

        @Override
        public boolean equals(final Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || !(obj instanceof PartValEqWrapper)) {
                return false;
            }
            final Partition p1 = this.partition;
            final Partition p2 = ((PartValEqWrapper) obj).partition;
            if (!p1.isSetValues() || !p2.isSetValues()) {
                return p1.isSetValues() == p2.isSetValues();
            }
            if (p1.getValues().size() != p2.getValues().size()) {
                return false;
            }
            for (int i = 0; i < p1.getValues().size(); ++i) {
                final String v1 = p1.getValues().get(i);
                final String v2 = p2.getValues().get(i);
                if (!Objects.equals(v1, v2)) {
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * Adds and drops partitions in one transaction.
     *
     * @param databaseName database name
     * @param tableName    table name
     * @param addParts     list of partitions
     * @param dropParts    list of partition values
     * @param deleteData   if true, deletes the data
     * @return true if successful
     * @throws NoSuchObjectException Exception if table does not exists
     * @throws MetaException         Exception if
     * @throws TException            any internal exception
     */
    @SuppressWarnings({"checkstyle:methodname"})
    @Override
    @SuppressFBWarnings(value = "NP_NULL_PARAM_DEREF", justification = "false positive")
    public boolean add_drop_partitions(final String databaseName,
                                       final String tableName, final List<Partition> addParts,
                                       final List<List<String>> dropParts, final boolean deleteData)
            throws NoSuchObjectException, MetaException, TException {
        startFunction("add_drop_partitions : db=" + databaseName + " tbl=" + tableName);
        if (addParts.size() == 0 && dropParts.size() == 0) {
            return true;
        }
        for (List<String> partVals : dropParts) {
            LOG.info("Drop Partition values:" + partVals);
        }
        for (Partition part : addParts) {
            LOG.info("Add Partition values:" + part);
        }

        boolean ret = false;
        Exception ex = null;
        try {
            ret = addDropPartitionsCore(getMS(), databaseName, tableName, addParts, dropParts, false);
        } catch (Exception e) {
            ex = e;
            if (e instanceof MetaException) {
                throw (MetaException) e;
            } else if (e instanceof InvalidObjectException) {
                throw (InvalidObjectException) e;
            } else if (e instanceof AlreadyExistsException) {
                throw (AlreadyExistsException) e;
            } else if (e instanceof NoSuchObjectException) {
                throw (NoSuchObjectException) e;
            } else {
                throw newMetaException(e);
            }
        } finally {
            endFunction("drop_partitions", ret, ex, tableName);
        }
        return ret;

    }

    private boolean addDropPartitionsCore(
            final RawStore ms, final String databaseName, final String tableName, final List<Partition> addParts,
            final List<List<String>> dropParts, final boolean ifNotExists)
            throws MetaException, InvalidObjectException, NoSuchObjectException, AlreadyExistsException,
            IOException, InvalidInputException, TException {
        logInfo("add_drop_partitions : db=" + databaseName + " tbl=" + tableName);
        boolean success = false;
        Table tbl = null;
        // Ensures that the list doesn't have dups, and keeps track of directories we have created.
        final Map<PartValEqWrapper, Boolean> addedPartitions = new HashMap<PartValEqWrapper, Boolean>();
        final List<Partition> existingParts = new ArrayList<Partition>();
        List<Partition> result = null;
        try {
            ms.openTransaction();
            tbl = get_table(databaseName, tableName);
            if (tbl == null) {
                throw new NoSuchObjectException("Unable to add partitions because "
                        + "database or table " + databaseName + "." + tableName + " does not exist");
            }
            // Drop the parts first
            dropPartitionsCoreNoTxn(ms, tbl, dropParts);

            // Now add the parts
            result = addPartitionsCoreNoTxn(ms, tbl, addParts, ifNotExists, addedPartitions, existingParts);

            if (!result.isEmpty() && !ms.addPartitions(databaseName, tableName, result)) {
                throw new MetaException("Unable to add partitions");
            }
            success = ms.commitTransaction();
        } finally {
            if (!success) {
                ms.rollbackTransaction();
                // Clean up the result of adding partitions
                for (Map.Entry<PartValEqWrapper, Boolean> e : addedPartitions.entrySet()) {
                    if (e.getValue()) {
                        getWh().deleteDir(new Path(e.getKey().partition.getSd().getLocation()), true);
                        // we just created this directory - it's not a case of pre-creation, so we nuke
                    }
                }
            }
        }
        return success;
    }

    private boolean startAddPartition(
            final RawStore ms, final Partition part, final boolean ifNotExists) throws MetaException, TException {
        MetaStoreUtils.validatePartitionNameCharacters(part.getValues(),
                partitionValidationPattern);
        final boolean doesExist = ms.doesPartitionExist(
                part.getDbName(), part.getTableName(), part.getValues());
        if (doesExist && !ifNotExists) {
            throw new AlreadyExistsException("Partition already exists: " + part);
        }
        return !doesExist;
    }

    /**
     * Handles the location for a partition being created.
     *
     * @param tbl  Table.
     * @param part Partition.
     * @return Whether the partition SD location is set to a newly created directory.
     */
    private boolean createLocationForAddedPartition(
            final Table tbl, final Partition part) throws MetaException {
        Path partLocation = null;
        String partLocationStr = null;
        if (part.getSd() != null) {
            partLocationStr = part.getSd().getLocation();
        }

        if (partLocationStr == null || partLocationStr.isEmpty()) {
            // set default location if not specified and this is
            // a physical table partition (not a view)
            if (tbl.getSd().getLocation() != null) {
                partLocation = new Path(tbl.getSd().getLocation(), Warehouse
                        .makePartName(tbl.getPartitionKeys(), part.getValues()));
            }
        } else {
            if (tbl.getSd().getLocation() == null) {
                throw new MetaException("Cannot specify location for a view partition");
            }
            partLocation = getWh().getDnsPath(new Path(partLocationStr));
        }

        boolean result = false;
        if (partLocation != null) {
            part.getSd().setLocation(partLocation.toString());
            final boolean doFileSystemCalls = getHiveConf().getBoolean("hive.metastore.use.fs.calls", true)
                || (tbl.getParameters() != null && Boolean.parseBoolean(tbl.getParameters()
                .getOrDefault("hive.metastore.use.fs.calls", "false")));
            if (doFileSystemCalls) {
                // Check to see if the directory already exists before calling
                // mkdirs() because if the file system is read-only, mkdirs will
                // throw an exception even if the directory already exists.
                if (!getWh().isDir(partLocation)) {
                    //
                    // Added to track the number of partition locations that do not exist before
                    // adding the partition metadata
                    registry.counter(HiveMetrics.CounterHivePartitionPathIsNotDir.getMetricName(),
                        "database", tbl.getDbName(), "table", tbl.getTableName()).increment();
                    logInfo(String.format("Partition location %s does not exist for table %s",
                        partLocation, tbl.getTableName()));
                    if (!getWh().mkdirs(partLocation, true)) {
                        throw new MetaException(partLocation + " is not a directory or unable to create one");
                    }
                }
                result = true;
            }
        }
        return result;
    }

    private void initializeAddedPartition(
            final Table tbl, final Partition part, final boolean madeDir) throws MetaException {
        initializeAddedPartition(tbl, new PartitionSpecProxy.SimplePartitionWrapperIterator(part), madeDir);
    }

    @SuppressFBWarnings
    private void initializeAddedPartition(
            final Table tbl, final PartitionSpecProxy.PartitionIterator part,
            final boolean madeDir) throws MetaException {
        // set create time
        final long time = System.currentTimeMillis() / 1000;
        part.setCreateTime((int) time);
        if (part.getParameters() == null || part.getParameters().get(hive_metastoreConstants.DDL_TIME) == null) {
            part.putToParameters(hive_metastoreConstants.DDL_TIME, Long.toString(time));
        }

        // Inherit table properties into partition properties.
        final Map<String, String> tblParams = tbl.getParameters();
        final String inheritProps = getHiveConf().getVar(HiveConf.ConfVars.METASTORE_PART_INHERIT_TBL_PROPS).trim();
        // Default value is empty string in which case no properties will be inherited.
        // * implies all properties needs to be inherited
        Set<String> inheritKeys = new HashSet<String>(Arrays.asList(inheritProps.split(",")));
        if (inheritKeys.contains("*")) {
            inheritKeys = tblParams.keySet();
        }

        for (String key : inheritKeys) {
            final String paramVal = tblParams.get(key);
            if (null != paramVal) { // add the property only if it exists in table properties
                part.putToParameters(key, paramVal);
            }
        }
    }

    private List<Partition> addPartitionsCoreNoTxn(
            final RawStore ms, final Table tbl, final List<Partition> parts, final boolean ifNotExists,
            final Map<PartValEqWrapper, Boolean> addedPartitions, final List<Partition> existingParts)
            throws MetaException, InvalidObjectException, AlreadyExistsException, TException {
        logInfo("add_partitions");
        final String dbName = tbl.getDbName();
        final String tblName = tbl.getTableName();
        final List<Partition> result = new ArrayList<Partition>();
        for (Partition part : parts) {
            if (!part.getTableName().equals(tblName) || !part.getDbName().equals(dbName)) {
                throw new MetaException("Partition does not belong to target table "
                        + dbName + "." + tblName + ": " + part);
            }
            final boolean shouldAdd = startAddPartition(ms, part, ifNotExists);
            if (!shouldAdd) {
                existingParts.add(part);
                LOG.info("Not adding partition " + part + " as it already exists");
                continue;
            }
            final boolean madeDir = createLocationForAddedPartition(tbl, part);
            if (addedPartitions.put(new PartValEqWrapper(part), madeDir) != null) {
                // Technically, for ifNotExists case, we could insert one and discard the other
                // because the first one now "exists", but it seems better to report the problem
                // upstream as such a command doesn't make sense.
                throw new MetaException("Duplicate partitions in the list: " + part);
            }
            initializeAddedPartition(tbl, part, madeDir);
            result.add(part);
        }
        return result;
    }

    private List<Partition> dropPartitionsCoreNoTxn(
            final RawStore ms, final Table tbl, final List<List<String>> partsValues)
            throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
        final List<Partition> deletedPartitions = new ArrayList<Partition>();
        Partition part = null;
        final String dbName = tbl.getDbName();
        final String tblName = tbl.getTableName();

        for (List<String> partValues : partsValues) {
            part = ms.getPartition(dbName, tblName, partValues);
            if (part == null) {
                throw new NoSuchObjectException("Partition doesn't exist. "
                        + partValues);
            }
            if (!ms.dropPartition(dbName, tblName, partValues)) {
                throw new MetaException("Unable to drop partition");
            }
            deletedPartitions.add(part);
        }
        return deletedPartitions;
    }
}
