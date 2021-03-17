package com.netflix.metacat.connector.hive.util;

import com.netflix.metacat.common.server.partition.util.PartitionUtil;
import com.netflix.metacat.common.server.partition.visitor.PartitionKeyParserEval;
import org.apache.hadoop.hive.common.FileUtils;

/**
 * Hive partition key evaluation.
 */
public class HivePartitionKeyParserEval extends PartitionKeyParserEval {
    @Override
    protected String toValue(final Object value) {
        return value == null ? PartitionUtil.DEFAULT_PARTITION_NAME
            : FileUtils.escapePathName(value.toString());
    }
}
