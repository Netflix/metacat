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

import com.google.common.collect.Maps;
import com.netflix.iceberg.expressions.Expression;
import com.netflix.iceberg.expressions.Expressions;
import com.netflix.iceberg.types.Types;
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

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * Iceberg Filter generator.
 */
public class IcebergFilterGenerator extends PartitionParserEval {
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

    /**
     * evalString.
     *
     * @param node node
     * @param data data
     * @return eval String
     */
    public Expression evalString(final SimpleNode node, final Object data) {
        final Object lhs = node.jjtGetChild(0).jjtAccept(this, data);
        final Compare comparison = (Compare) node.jjtGetChild(1).jjtAccept(this, data);
        final Object rhs = node.jjtGetChild(2).jjtAccept(this, data);
        return createIcebergFilter(lhs, rhs, comparison);
    }

    private boolean isField(final String key) {
        return fieldMap.containsKey(key.toLowerCase());
    }

    private Expression createIcebergFilter(final Object lhs,
                                           final Object rhs,
                                           final Compare comparison) {
        String key = null;
        Object value = null;
        //
        // lhs, rhs or both can be keys
        //
        if (lhs instanceof String && isField((String) lhs)) {
            key = lhs.toString();
            value = rhs;
        } else if (rhs instanceof String && isField((String) rhs)) {
            key = rhs.toString();
            value = lhs;
        }
        if (key == null || value == null) {
            throw new RuntimeException("Invalid expression key/value " + lhs + "/" + rhs);
        }

        //the parser changes numerical to bigdecimal
        if (value instanceof BigDecimal) {
            switch (fieldMap.get(key).type().typeId()) {
                case LONG:
                    value = ((BigDecimal) value).longValue();
                    break;
                case INTEGER:
                    value = ((BigDecimal) value).intValue();
                    break;
                case DOUBLE:
                    value = ((BigDecimal) value).doubleValue();
                    break;
                case FLOAT:
                    value = ((BigDecimal) value).floatValue();
                    break;
                case DECIMAL:
                    break;
                default:
                    throw new RuntimeException("Unsupported BigDecimal to Iceberg Type");
            }
        }

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
                throw new RuntimeException("Not supported");
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
        final Expression compare1 = createIcebergFilter(value, startValue, node.not ? Compare.LT : Compare.GTE);
        final Expression compare2 = createIcebergFilter(value, endValue, node.not ? Compare.GT : Compare.LTE);
        return (node.not)
            ? Expressions.or(compare1, compare2) : Expressions.and(compare1, compare2);
    }

    @Override
    public Object visit(final ASTIN node, final Object data) {
        throw new RuntimeException("Not supported");
    }

    @Override
    public Object visit(final ASTMATCHES node, final Object data) {
        throw new RuntimeException("Not supported");
    }

    @Override
    public Object visit(final ASTNOT node, final Object data) {
        throw new RuntimeException("Not supported");
    }

    @Override
    public Object visit(final ASTLIKE node, final Object data) {
        throw new RuntimeException("Not supported");
    }

    private Expression evalSingleTerm(final ASTCOMPARE node, final Object data) {
        final Object value = node.jjtGetChild(0).jjtAccept(this, data);
        if (value != null) {
            return Boolean.parseBoolean(value.toString())
                ? Expressions.alwaysTrue() : Expressions.alwaysFalse();
        }
        return Expressions.alwaysFalse();
    }
}
