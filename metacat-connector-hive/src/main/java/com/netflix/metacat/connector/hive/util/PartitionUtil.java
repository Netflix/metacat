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
package com.netflix.metacat.connector.hive.util;

import com.google.common.base.Strings;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.connectors.exception.InvalidMetaException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Utility class for partitions.
 * @author amajumdar
 */
public final class PartitionUtil {

    private PartitionUtil() {
    }

    /**
     * Retrieves the partition values from the partition name. This method also validates the partition keys to that
     * of the table.
     *
     * @param tableQName  table name
     * @param table       table
     * @param partName    partition name
     * @return list of partition values
     */
    public static List<String> getPartValuesFromPartName(final QualifiedName tableQName, final Table table,
        final String partName) {
        if (Strings.isNullOrEmpty(partName)) {
            throw new InvalidMetaException(tableQName, partName, null);
        }
        final LinkedHashMap<String, String> partSpec = new LinkedHashMap<>();
        Warehouse.makeSpecFromName(partSpec, new Path(partName));
        final List<String> values = new ArrayList<>();
        for (FieldSchema field : table.getPartitionKeys()) {
            final String key = field.getName();
            final String val = partSpec.get(key);
            if (val == null) {
                throw new InvalidMetaException(tableQName, partName, null);
            }
            values.add(val);
        }
        return values;
    }

    /**
     * Escape partition name.
     *
     * @param partName    partition name
     * @return Escaped partition name
     */
    public static String escapePartitionName(final String partName) {
        final LinkedHashMap<String, String> partSpec = new LinkedHashMap<>();
        Warehouse.makeSpecFromName(partSpec, new Path(partName));
        return FileUtils.makePartName(new ArrayList<>(partSpec.keySet()), new ArrayList<>(partSpec.values()));
    }

    /**
     * Generate partition name from the <code>partValues</code>.
     *
     * @param partitionKeys list of partition keys
     * @param partValues list of partition values
     * @return partition name
     */
    public static String makePartName(final List<FieldSchema> partitionKeys, final List<String> partValues) {
        try {
            return Warehouse.makePartName(partitionKeys, partValues);
        } catch (MetaException e) {
            throw new InvalidMetaException("Failed making the part name from the partition values", e);
        }
    }
}
