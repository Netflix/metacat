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

package com.netflix.metacat.common.partition.visitor;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.netflix.metacat.common.partition.parser.ASTAND;
import com.netflix.metacat.common.partition.parser.ASTBETWEEN;
import com.netflix.metacat.common.partition.parser.ASTBOOLEAN;
import com.netflix.metacat.common.partition.parser.ASTCOMPARE;
import com.netflix.metacat.common.partition.parser.ASTEQ;
import com.netflix.metacat.common.partition.parser.ASTFILTER;
import com.netflix.metacat.common.partition.parser.ASTGT;
import com.netflix.metacat.common.partition.parser.ASTGTE;
import com.netflix.metacat.common.partition.parser.ASTIN;
import com.netflix.metacat.common.partition.parser.ASTLIKE;
import com.netflix.metacat.common.partition.parser.ASTLT;
import com.netflix.metacat.common.partition.parser.ASTLTE;
import com.netflix.metacat.common.partition.parser.ASTMATCHES;
import com.netflix.metacat.common.partition.parser.ASTNEQ;
import com.netflix.metacat.common.partition.parser.ASTNOT;
import com.netflix.metacat.common.partition.parser.ASTNUM;
import com.netflix.metacat.common.partition.parser.ASTOR;
import com.netflix.metacat.common.partition.parser.ASTSTRING;
import com.netflix.metacat.common.partition.parser.ASTVAR;
import com.netflix.metacat.common.partition.parser.PartitionParserVisitor;
import com.netflix.metacat.common.partition.parser.SimpleNode;
import com.netflix.metacat.common.partition.parser.Variable;

import java.math.BigDecimal;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Partition Expression Visitor.
 */
public class PartitionParserEval implements PartitionParserVisitor {
    /** Like patterns. */
    public static final Pattern LIKE_PATTERN = Pattern.compile("(\\[%\\]|\\[_\\]|\\[\\[\\]|%|_)");
    /** LIKE to Regex token replacements. */
    public static final Map<String, String> LIKE_TO_REGEX_REPLACEMENTS = new ImmutableMap.Builder<String, String>()
        .put("[%]", "%")
        .put("[_]", "_")
        .put("[[]", "[")
        .put("%", ".*")
        .put("_", ".").build();

    /** Compare enum. */
    public enum Compare {
        /** Compare. */
        EQ("="), GT(">"), GTE(">="), LT("<"), LTE("<="), NEQ("!="), MATCHES("MATCHES"), LIKE("LIKE");
        private String expression;

        Compare(final String expression) {
            this.expression = expression;
        }

        public String getExpression() {
            return expression;
        }
    }

    private Map<String, String> context;

    /**
     * Constructor.
     */
    public PartitionParserEval() {
        this(Maps.newHashMap());
    }

    /**
     * Constructor.
     * @param context context parameters
     */
    public PartitionParserEval(final Map<String, String> context) {
        this.context = context;
    }

    /**
     * Compares.
     * @param node node in the tree
     * @param data data
     * @return comparison result
     */
    public Boolean evalCompare(final SimpleNode node, final Object data) {
        final Object value1 = node.jjtGetChild(0).jjtAccept(this, data);
        final Compare comparison = (Compare) node.jjtGetChild(1).jjtAccept(this, data);
        final Object value2 = node.jjtGetChild(2).jjtAccept(this, data);
        return compare(comparison, value1, value2);
    }

    /**
     * Compare value1 and value2.
     * @param comparison comparison expression
     * @param value1 value
     * @param value2 value
     * @return comparison result
     */
    @SuppressWarnings({ "unchecked", "rawtypes", "checkstyle:methodname" })
    public boolean compare(final Compare comparison, final Object value1, final Object value2) {
        if (value1 == null) {
            switch (comparison) {
            case EQ:
            case MATCHES:
            case LIKE:
                return value2 == null;
            case NEQ:
                return value2 != null;
            default:
                return false;
            }
        }
        if (value2 instanceof String) {
            return _compare(comparison, value1.toString(), value2.toString());
        }
        if (value2 instanceof BigDecimal) {
            final BigDecimal valueDecimal = new BigDecimal(value1.toString());
            return _compare(comparison, valueDecimal, (BigDecimal) value2);
        }
        if (value1 instanceof Comparable && value2 instanceof Comparable) {
            return _compare(comparison, (Comparable) value1, (Comparable) value2);
        }
        throw new IllegalStateException("error processing partition filter");
    }

    @SuppressWarnings({ "unchecked", "rawtypes", "checkstyle:methodname" })
    private boolean _compare(final Compare comparison, final Comparable value1, final Comparable value2) {
        if (comparison.equals(Compare.MATCHES) || comparison.equals(Compare.LIKE)) {
            if (value2 != null) {
                String value = value2.toString();
                if (comparison.equals(Compare.LIKE)) {
                    value = sqlLiketoRegexExpression(value);
                }
                return value1.toString().matches(value);
            }
        } else {
            final int compare = value1.compareTo(value2);
            switch (comparison) {
            case GT:
                return compare > 0;
            case GTE:
                return compare >= 0;
            case LT:
                return compare < 0;
            case LTE:
                return compare <= 0;
            case EQ:
                return compare == 0;
            case NEQ:
                return compare != 0;
            default:
                return false;
            }
        }
        return false;
    }

    //TODO: Need to escape regex meta characters
    protected String sqlLiketoRegexExpression(final String likeExpression) {
        final Matcher m = LIKE_PATTERN.matcher(likeExpression);

        final StringBuffer builder = new StringBuffer();
        while (m.find()) {
            m.appendReplacement(builder, LIKE_TO_REGEX_REPLACEMENTS.get(m.group()));
        }
        m.appendTail(builder);
        return builder.toString();
    }

    @Override
    public Object visit(final ASTAND node, final Object data) {
        final Boolean v1 = (Boolean) node.jjtGetChild(0).jjtAccept(this, data);
        return v1 && (Boolean) node.jjtGetChild(1).jjtAccept(this, data);
    }

    @Override
    public Object visit(final ASTEQ node, final Object data) {
        return Compare.EQ;
    }

    @Override
    public Object visit(final ASTBETWEEN node, final Object data) {
        final Object value = node.jjtGetChild(0).jjtAccept(this, data);
        final Object startValue = node.jjtGetChild(1).jjtAccept(this, data);
        final Object endValue = node.jjtGetChild(2).jjtAccept(this, data);
        final boolean compare1 = compare(Compare.GTE, value, startValue);
        final boolean compare2 = compare(Compare.LTE, value, endValue);
        final boolean result = compare1 && compare2;
        return node.not != result;
    }

    @Override
    public Object visit(final ASTIN node, final Object data) {
        Object value = node.jjtGetChild(0).jjtAccept(this, data);
        boolean result = false;
        for (int i = 1; i < node.jjtGetNumChildren(); i++) {
            final Object inValue = node.jjtGetChild(i).jjtAccept(this, data);
            if (value != null && inValue instanceof BigDecimal) {
                value = new BigDecimal(value.toString());
            }
            if ((value == null && inValue == null)
                || (value != null && value.equals(inValue))) {
                result = true;
                break;
            }
        }
        return node.not != result;
    }

    @Override
    public Object visit(final ASTCOMPARE node, final Object data) {
        if (node.jjtGetNumChildren() == 1) {
            return evalSingleTerm(node, data);
        } else {
            return evalCompare(node, data);
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
    public Object visit(final ASTBOOLEAN node, final Object data) {
        return Boolean.parseBoolean(node.jjtGetValue().toString());
    }

    @Override
    public Object visit(final ASTFILTER node, final Object data) {
        return node.jjtGetChild(0).jjtAccept(this, data);
    }

    @Override
    public Object visit(final ASTGT node, final Object data) {
        return Compare.GT;
    }

    @Override
    public Object visit(final ASTGTE node, final Object data) {
        return Compare.GTE;
    }

    @Override
    public Object visit(final ASTLT node, final Object data) {
        return Compare.LT;
    }

    @Override
    public Object visit(final ASTLTE node, final Object data) {
        return Compare.LTE;
    }

    @Override
    public Object visit(final ASTNEQ node, final Object data) {
        return Compare.NEQ;
    }

    @Override
    public Object visit(final ASTMATCHES node, final Object data) {
        return Compare.MATCHES;
    }

    @Override
    public Object visit(final ASTLIKE node, final Object data) {
        final Object value1 = node.jjtGetChild(0).jjtAccept(this, data);
        final Object value2 = node.jjtGetChild(1).jjtAccept(this, data);
        final boolean result = compare(Compare.LIKE, value1, value2);
        return node.not != result;
    }

    @Override
    public Object visit(final ASTNUM node, final Object data) {
        return node.jjtGetValue();
    }

    @Override
    public Object visit(final ASTOR node, final Object data) {
        final Boolean v1 = (Boolean) node.jjtGetChild(0).jjtAccept(this, data);
        return v1 || (Boolean) node.jjtGetChild(1).jjtAccept(this, data);
    }

    @Override
    public Object visit(final ASTNOT node, final Object data) {
        return !(Boolean) node.jjtGetChild(0).jjtAccept(this, data);
    }

    @Override
    public Object visit(final ASTSTRING node, final Object data) {
        return node.jjtGetValue();
    }

    @Override
    public Object visit(final ASTVAR node, final Object data) {
        if (!context.containsKey(((Variable) node.jjtGetValue()).getName())) {
            throw new IllegalArgumentException("Missing variable: " + ((Variable) node.jjtGetValue()).getName());
        }
        return context.get(((Variable) node.jjtGetValue()).getName());
    }

    @Override
    public Object visit(final SimpleNode node, final Object data) {
        return null;
    }

}
