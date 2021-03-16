package com.netflix.metacat.connector.hive.util;

import com.netflix.metacat.common.server.partition.util.FilterPartition;
import org.apache.hadoop.hive.common.FileUtils;

import java.util.Map;

/**
 * Filter partition for hive.
 *
 * @author amajumdar
 */
public class HiveFilterPartition extends FilterPartition {
    @Override
    protected void addNameValues(final String name, final Map<String, String> values) {
        super.addNameValues(name, values);
        values.replaceAll((key, value) -> value == null ? null : FileUtils.unescapePathName(value));
    }
}
