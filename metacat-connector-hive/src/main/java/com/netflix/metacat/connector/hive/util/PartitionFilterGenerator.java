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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.metacat.common.server.partition.parser.ASTAND;
import com.netflix.metacat.common.server.partition.parser.ASTBETWEEN;
import com.netflix.metacat.common.server.partition.parser.ASTCOMPARE;
import com.netflix.metacat.common.server.partition.parser.ASTIN;
import com.netflix.metacat.common.server.partition.parser.ASTLIKE;
import com.netflix.metacat.common.server.partition.parser.ASTMATCHES;
import com.netflix.metacat.common.server.partition.parser.ASTNOT;
import com.netflix.metacat.common.server.partition.parser.ASTNULL;
import com.netflix.metacat.common.server.partition.parser.ASTOR;
import com.netflix.metacat.common.server.partition.parser.ASTVAR;
import com.netflix.metacat.common.server.partition.parser.SimpleNode;
import com.netflix.metacat.common.server.partition.parser.Variable;
import com.netflix.metacat.common.server.partition.util.PartitionUtil;
import com.netflix.metacat.common.server.partition.visitor.PartitionParserEval;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde.serdeConstants;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;

/**
 * PartitionFilterGenerator.
 *
 * @author zhenl
 * @since 1.0.0
 */
public class PartitionFilterGenerator extends PartitionParserEval {
    private final Map<String, PartitionCol> partitionColumns;
    private final List<Object> params;
    private List<String> partVals;
    private boolean optimized;
    private final boolean escapePartitionNameOnFilter;

    /**
     * Constructor.
     *
     * @param partitionsKeys partition keys
     * @param escapePartitionNameOnFilter if true, escape the partition name
     */
    public PartitionFilterGenerator(final List<FieldSchema> partitionsKeys, final boolean escapePartitionNameOnFilter) {
        partitionColumns = Maps.newHashMap();
        this.partVals = Lists.newArrayListWithCapacity(partitionsKeys.size());
        for (int index = 0; index < partitionsKeys.size(); index++) {
            final FieldSchema partitionKey = partitionsKeys.get(index);
            partitionColumns.put(partitionKey.getName().toLowerCase(), new PartitionCol(index, partitionKey.getType()));
            this.partVals.add(null);
        }
        this.params = Lists.newArrayList();
        this.optimized = true;
        this.escapePartitionNameOnFilter = escapePartitionNameOnFilter;
    }

    /**
     * evalString.
     *
     * @param node node
     * @param data data
     * @return eval String
     */
    public String evalString(final SimpleNode node, final Object data) {
        final Object lhs = node.jjtGetChild(0).jjtAccept(this, data);
        final Compare comparison = (Compare) node.jjtGetChild(1).jjtAccept(this, data);
        final Object rhs = node.jjtGetChild(2).jjtAccept(this, data);
        return createSqlCriteria(lhs, rhs, comparison, false);
    }

    private String createSqlCriteria(final Object lhs, final Object rhs, final Compare comparison, final boolean not) {
        String key = null;
        Object value = null;
        boolean isKeyLhs = true;
        //
        // lhs, rhs or both can be keys
        //
        if (lhs instanceof String && isKey((String) lhs)) {
            key = lhs.toString();
            value = rhs;
        } else if (rhs instanceof String && isKey((String) rhs)) {
            key = rhs.toString();
            value = lhs;
            isKeyLhs = false;
        }
        if (key == null || value == null) {
            throw new RuntimeException("Invalid expression key/value " + lhs + "/" + rhs);
        }

        final PartitionCol partCol = partitionColumns.get(key.toLowerCase());
        final String valueStr = value.toString();
        final String operator = not ? "not " + comparison.getExpression() : comparison.getExpression();
        if (partCol != null && valueStr != null && (partitionColumns.containsKey(valueStr.toLowerCase()))) {
            // Key part column
            partCol.occurred();
            final FilterType colType = partCol.type;
            optimized = false;
            // Value part column
            final PartitionCol valuePartCol = partitionColumns.get(valueStr);
            valuePartCol.occurred();
            final FilterType valueColType = valuePartCol.type;
            if (colType != valueColType) {
                throw new RuntimeException(
                        String.format("Invalid column comparison with key as %s and"
                                + " value as %s", colType, valueColType));
            }
            return String.format("%s %s %s", getSQLExpression(partCol), operator, getSQLExpression(valuePartCol));
        } else if (partCol != null) {
            partCol.occurred();
            // For more optimization
            if (partCol.hasOccurredOnlyOnce() && Compare.EQ.equals(comparison)) {
                partVals.set(partCol.index, key + "="
                    + (escapePartitionNameOnFilter ? FileUtils.escapePathName(valueStr) : valueStr));
            } else {
                optimized = false;
            }
            final FilterType colType = partCol.type;
            if (colType == FilterType.Invalid) {
                throw new RuntimeException("Invalid type " + colType);
            }
            FilterType valType = FilterType.fromClass(value);
            if (valType == FilterType.Invalid) {
                throw new RuntimeException("Invalid value " + value.getClass());
            }

            if (colType == FilterType.Date && valType == FilterType.String) {
                try {
                    value = new java.sql.Date(
                            HiveMetaStore.PARTITION_DATE_FORMAT.get().parse((String) value).getTime());
                    valType = FilterType.Date;
                } catch (ParseException pe) { // do nothing, handled below - types will mismatch
                }
            } else if (colType == FilterType.Timestamp && valType == FilterType.String) {
                try {
                    value = new java.sql.Timestamp(
                        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse((String) value).getTime());
                    valType = FilterType.Timestamp;
                } catch (ParseException pe) { // do nothing, handled below - types will mismatch
                }
            } else if (colType == FilterType.Integral && valType == FilterType.String) {
                try {
                    value = new BigDecimal((String) value);
                    valType = FilterType.Integral;
                } catch (NumberFormatException pe) { // do nothing, handled below - types will mismatch
                }
            } else if (colType == FilterType.String && valType != FilterType.String) {
                value = value.toString();
                valType = FilterType.String;
            }

            if (colType != valType) {
                throw new RuntimeException("Invalid value " + value.getClass());
            }

            key = getSQLExpression(partCol);
            params.add(value);
        } else if ("batchid".equalsIgnoreCase(key)) {
            return "1=1";
        } else if ("dateCreated".equalsIgnoreCase(key)) {
            optimized = false;
            key = "p.CREATE_TIME";
            params.add(value);
        } else {
            throw new RuntimeException("Invalid expression key " + key);
        }
        return isKeyLhs ? String.format("%s %s %s", key, operator, "?")
                : String.format("%s %s %s", "?", operator, key);

    }

    private String getSQLExpression(final PartitionCol partCol) {
        String result = "pv" + partCol.index + ".part_key_val";
        if (partCol.type != FilterType.String) {
            if (partCol.type == FilterType.Integral) {
                result = "cast(" + result + " as decimal(21,0))";
            } else if (partCol.type == FilterType.Date) {
                result = "cast(" + result + " as date)";
            } else if (partCol.type == FilterType.Timestamp) {
                result = "cast(" + result + " as timestamp)";
            }
        }
        return result;
    }

    private boolean isKey(final String key) {
        return partitionColumns.containsKey(
                key.toLowerCase()) || "batchid".equalsIgnoreCase(key) || "dateCreated".equalsIgnoreCase(key);
    }

    public List<Object> getParams() {
        return params;
    }

    /**
     * joinSQL.
     *
     * @return joined sql
     */
    public String joinSql() {
        final StringBuilder result = new StringBuilder();
        if (!isOptimized()) {
            partitionColumns.values().forEach(partCol -> {
                if (partCol.hasOccurred()) {
                    final String tableAlias = "pv" + partCol.index;
                    result.append(" join PARTITION_KEY_VALS as ").append(tableAlias)
                            .append(" on p.part_id=").append(tableAlias).append(".part_id and ")
                            .append(tableAlias).append(".integer_idx=").append(partCol.index);
                }
            });
        }
        return result.toString();
    }

    public boolean isOptimized() {
        return optimized;
    }

    /**
     * getOptimizedSql.
     *
     * @return get Optimized Sql
     */
    public String getOptimizedSql() {
        final StringBuilder result = new StringBuilder();
        boolean likeExpression = false;
        boolean emptyPartVals = true;
        if (isOptimized()) {
            for (int i = 0; i < partVals.size(); i++) {
                final String partVal = partVals.get(i);
                if (partVal == null) {
                    likeExpression = true;
                    result.append("%");
                } else {
                    emptyPartVals = false;
                    result.append(partVal);
                    if (i + 1 != partVals.size()) {
                        result.append("/");
                    }
                }
            }
        }
        if (emptyPartVals) {
            return result.toString();
        } else if (likeExpression) {
            params.clear();
            params.add(result.toString());
            return "p.part_name like ?";
        } else {
            params.clear();
            params.add(result.toString());
            return "p.part_name = ?";
        }
    }

    @Override
    public Object visit(final ASTAND node, final Object data) {
        return String.format("(%s %s %s)", node.jjtGetChild(0).jjtAccept(this, data), "and",
                node.jjtGetChild(1).jjtAccept(this, data));
    }

    @Override
    public Object visit(final ASTOR node, final Object data) {
        optimized = false;
        return String.format("(%s %s %s)", node.jjtGetChild(0).jjtAccept(this, data), "or",
                node.jjtGetChild(1).jjtAccept(this, data));
    }

    @Override
    public Object visit(final ASTCOMPARE node, final Object data) {
        if (node.jjtGetNumChildren() == 1) {
            return evalSingleTerm(node, data).toString();
        } else {
            return evalString(node, data);
        }
    }

    private Boolean evalSingleTerm(final ASTCOMPARE node, final Object data) {
        Boolean result = Boolean.FALSE;
        final Object value = node.jjtGetChild(0).jjtAccept(this, data);
        if (value != null) {
            result = Boolean.parseBoolean(value.toString());
        }
        return result;
    }

    @Override
    public Object visit(final ASTBETWEEN node, final Object data) {
        final Object value = node.jjtGetChild(0).jjtAccept(this, data);
        final Object startValue = node.jjtGetChild(1).jjtAccept(this, data);
        final Object endValue = node.jjtGetChild(2).jjtAccept(this, data);
        final String compare1 = createSqlCriteria(value, startValue, node.not ? Compare.LT : Compare.GTE, false);
        final String compare2 = createSqlCriteria(value, endValue, node.not ? Compare.GT : Compare.LTE, false);
        return String.format("(%s %s %s)", compare1, node.not ? "or" : "and", compare2);
    }

    @Override
    public Object visit(final ASTIN node, final Object data) {
        final Object lhs = node.jjtGetChild(0).jjtAccept(this, data);
        final StringBuilder builder = new StringBuilder();
        for (int i = 1; i < node.jjtGetNumChildren(); i++) {
            final Object inValue = node.jjtGetChild(i).jjtAccept(this, data);
            if (i != 1) {
                builder.append(",");
            }
            if (inValue instanceof String) {
                builder.append("'").append(inValue).append("'");
            } else {
                builder.append(inValue);
            }
        }
        final PartitionCol partCol = partitionColumns.get(lhs.toString().toLowerCase());
        if (partCol != null) {
            partCol.occurred();
            optimized = false;
            final String operator = node.not ? "not in" : "in";
            return String.format("%s %s (%s)", getSQLExpression(partCol), operator, builder.toString());
        } else {
            throw new RuntimeException("Invalid expression key " + lhs);
        }
    }

    @Override
    public Object visit(final ASTLIKE node, final Object data) {
        final Object lhs = node.jjtGetChild(0).jjtAccept(this, data);
        final Object rhs = node.jjtGetChild(1).jjtAccept(this, data);
        return createSqlCriteria(lhs, rhs, Compare.LIKE, node.not);
    }

    @Override
    public Object visit(final ASTNULL node, final Object data) {
        final Object lhs = node.jjtGetChild(0).jjtAccept(this, data);
        return createSqlCriteria(lhs, PartitionUtil.DEFAULT_PARTITION_NAME, Compare.EQ, node.not);
    }

    @Override
    public Object visit(final ASTVAR node, final Object data) {
        return ((Variable) node.jjtGetValue()).getName();
    }

    @Override
    public Object visit(final ASTMATCHES node, final Object data) {
        throw new RuntimeException("Not supported");
    }

    @Override
    public Object visit(final ASTNOT node, final Object data) {
        throw new RuntimeException("Not supported");
    }

    private enum FilterType {
        Integral,
        String,
        Date,
        Timestamp,
        Invalid;

        static FilterType fromType(final String colTypeStr) {
            if (colTypeStr.equals(serdeConstants.STRING_TYPE_NAME)) {
                return FilterType.String;
            } else if (colTypeStr.equals(serdeConstants.DATE_TYPE_NAME)) {
                return FilterType.Date;
            } else if (colTypeStr.equals(serdeConstants.TIMESTAMP_TYPE_NAME)) {
                return FilterType.Timestamp;
            } else if (serdeConstants.IntegralTypes.contains(colTypeStr)) {
                return FilterType.Integral;
            }
            return FilterType.Invalid;
        }

        public static FilterType fromClass(final Object value) {
            if (value instanceof String) {
                return FilterType.String;
            } else if (value instanceof Number) {
                return FilterType.Integral;
            } else if (value instanceof java.sql.Date) {
                return FilterType.Date;
            } else if (value instanceof java.sql.Timestamp) {
                return FilterType.Timestamp;
            }
            return FilterType.Invalid;
        }
    }

    static class PartitionCol {
        private int index;
        private FilterType type;
        private int occurrences;

        PartitionCol(final int index, final String type) {
            this.index = index;
            this.type = FilterType.fromType(type);
        }

        void occurred() {
            occurrences++;
        }

        boolean hasOccurred() {
            return occurrences > 0;
        }

        boolean hasOccurredOnlyOnce() {
            return occurrences == 1;
        }
    }
}
