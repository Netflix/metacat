/*
 *  Copyright 2018 Netflix, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.netflix.metacat.connector.hive.util;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import com.netflix.metacat.common.server.partition.parser.ASTAND;
import com.netflix.metacat.common.server.partition.parser.ASTBETWEEN;
import com.netflix.metacat.common.server.partition.parser.ASTCOMPARE;
import com.netflix.metacat.common.server.partition.parser.ASTIN;
import com.netflix.metacat.common.server.partition.parser.ASTLIKE;
import com.netflix.metacat.common.server.partition.parser.ASTMATCHES;
import com.netflix.metacat.common.server.partition.parser.ASTNOT;
import com.netflix.metacat.common.server.partition.parser.ASTOR;
import com.netflix.metacat.common.server.partition.parser.ASTVAR;
import com.netflix.metacat.common.server.partition.parser.SimpleNode;
import com.netflix.metacat.common.server.partition.parser.Variable;
import com.netflix.metacat.common.server.partition.visitor.PartitionParserEval;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Iceberg Filter generator.
 */
public class IcebergFilterGenerator extends PartitionParserEval {
    private static final Set<String> ICEBERG_TIMESTAMP_NAMES
        = ImmutableSet.of("dateCreated", "lastUpdated");
    private final Map<String, Types.NestedField> fieldMap;

    /**
     * Constructor.
     *
     * @param fields partition fields
     */
    public IcebergFilterGenerator(final List<Types.NestedField> fields) {
        fieldMap = Maps.newHashMap();
        for (final Types.NestedField field : fields) {
            fieldMap.put(field.name(), field);
        }
    }

    @Override
    public Object visit(final ASTAND node, final Object data) {
        return Expressions.and((Expression) node.jjtGetChild(0).jjtAccept(this, data),
            (Expression) node.jjtGetChild(1).jjtAccept(this, data));

    }

    @Override
    public Object visit(final ASTOR node, final Object data) {
        return Expressions.or((Expression) node.jjtGetChild(0).jjtAccept(this, data),
            (Expression) node.jjtGetChild(1).jjtAccept(this, data));
    }

    @Override
    public Object visit(final ASTCOMPARE node, final Object data) {
        if (node.jjtGetNumChildren() == 1) {
            return evalSingleTerm(node, data).toString();
        } else {
            return evalString(node, data);
        }
    }

    @Override
    public Object visit(final ASTVAR node, final Object data) {
        return ((Variable) node.jjtGetValue()).getName();
    }

    @Override
    public Object visit(final ASTBETWEEN node, final Object data) {
        final Object value = node.jjtGetChild(0).jjtAccept(this, data);
        final Object startValue = node.jjtGetChild(1).jjtAccept(this, data);
        final Object endValue = node.jjtGetChild(2).jjtAccept(this, data);
        final Expression compare1 =
            createIcebergExpression(value, startValue, node.not ? Compare.LT : Compare.GTE);
        final Expression compare2 =
            createIcebergExpression(value, endValue, node.not ? Compare.GT : Compare.LTE);
        return (node.not)
            ? Expressions.or(compare1, compare2) : Expressions.and(compare1, compare2);
    }

    @Override
    public Object visit(final ASTIN node, final Object data) {
        throw new UnsupportedOperationException("IN Operator not supported");
    }

    @Override
    public Object visit(final ASTMATCHES node, final Object data) {
        throw new UnsupportedOperationException("Operator Not supported");
    }

    @Override
    public Object visit(final ASTNOT node, final Object data) {
        throw new UnsupportedOperationException("Operator Not supported");
    }

    @Override
    public Object visit(final ASTLIKE node, final Object data) {
        throw new UnsupportedOperationException("Not supported");
    }

    private Expression evalSingleTerm(final ASTCOMPARE node, final Object data) {
        final Object value = node.jjtGetChild(0).jjtAccept(this, data);
        if (value != null) {
            return Boolean.parseBoolean(value.toString())
                ? Expressions.alwaysTrue() : Expressions.alwaysFalse();
        }
        return Expressions.alwaysFalse();
    }

    /**
     * evalString.
     *
     * @param node node
     * @param data data
     * @return eval String
     */
    private Expression evalString(final SimpleNode node, final Object data) {
        final Object lhs = node.jjtGetChild(0).jjtAccept(this, data);
        final Compare comparison = (Compare) node.jjtGetChild(1).jjtAccept(this, data);
        final Object rhs = node.jjtGetChild(2).jjtAccept(this, data);
        return createIcebergExpression(lhs, rhs, comparison);
    }

    /**
     * Check if the key is part of field.
     *
     * @param key input string
     * @return True if key is a field.
     */
    private boolean isField(final Object key) {
        return (key instanceof String) && fieldMap.containsKey(((String) key).toLowerCase());
    }

    /**
     * Check if the key is an iceberg supported date filter field.
     *
     * @param key input string
     * @return True if key is an iceberg supported date filter field.
     */
    private boolean isIcebergTimestamp(final Object key) {
        return (key instanceof String) && ICEBERG_TIMESTAMP_NAMES.contains(key);
    }

    /**
     * Get the key and value field of iceberg expression.
     *
     * @param lhs left hand string
     * @param rhs right hand string
     * @return key value pair for iceberg expression.
     */
    private Pair<String, Object> getExpressionKeyValue(final Object lhs,
                                                       final Object rhs) {
        if (isIcebergTimestamp(lhs)) {
            return new ImmutablePair<>(lhs.toString(), ((BigDecimal) rhs).longValue());
        } else if (isIcebergTimestamp(rhs)) {
            return new ImmutablePair<>(rhs.toString(), ((BigDecimal) lhs).longValue());
        }
        if (isField(lhs)) {
            return new ImmutablePair<>(lhs.toString(), getValue(lhs.toString(), rhs));
        } else if (isField(rhs)) {
            return new ImmutablePair<>(rhs.toString(), getValue(rhs.toString(), lhs));
        }
        throw new IllegalArgumentException(
            String.format("Invalid input \"%s/%s\" filter must be columns in fields %s or %s",
                lhs, rhs, fieldMap.keySet().toString(), ICEBERG_TIMESTAMP_NAMES.toString()));
    }

    /**
     * Transform the value type to iceberg type.
     *
     * @param key   the input filter key
     * @param value the input filter value
     * @return iceberg type
     */
    private Object getValue(final String key, final Object value) {
        if (value instanceof BigDecimal) {
            switch (fieldMap.get(key).type().typeId()) {
                case LONG:
                    return ((BigDecimal) value).longValue();
                case INTEGER:
                    return ((BigDecimal) value).intValue();
                case DOUBLE:
                    return ((BigDecimal) value).doubleValue();
                case FLOAT:
                    return ((BigDecimal) value).floatValue();
                case DECIMAL:
                    return value;
                default:
                    throw new IllegalArgumentException("Cannot convert the given big decimal value to an Iceberg type");
            }
        }
        return value;
    }

    /**
     * Based on filter create iceberg expression.
     *
     * @param lhs        left hand string
     * @param rhs        right hand string
     * @param comparison comparing operator
     * @return iceberg expression
     */
    private Expression createIcebergExpression(final Object lhs,
                                               final Object rhs,
                                               final Compare comparison) {
        final Pair<String, Object> keyValue = getExpressionKeyValue(lhs, rhs);
        final String key = keyValue.getLeft();
        final Object value = keyValue.getRight();
        switch (comparison) {
            case EQ:
                return Expressions.equal(key, value);
            case LTE:
                return Expressions.lessThanOrEqual(key, value);
            case GTE:
                return Expressions.greaterThanOrEqual(key, value);
            case GT:
                return Expressions.greaterThan(key, value);
            case LT:
                return Expressions.lessThan(key, value);
            case NEQ:
                return Expressions.notEqual(key, value);
            default:
                throw new UnsupportedOperationException(String.format("Operator %s supported", comparison));
        }
    }
}
