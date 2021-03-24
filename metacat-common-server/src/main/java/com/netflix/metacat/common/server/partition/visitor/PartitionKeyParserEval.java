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

package com.netflix.metacat.common.server.partition.visitor;

import com.google.common.collect.Sets;
import com.netflix.metacat.common.server.partition.parser.ASTAND;
import com.netflix.metacat.common.server.partition.parser.ASTBETWEEN;
import com.netflix.metacat.common.server.partition.parser.ASTCOMPARE;
import com.netflix.metacat.common.server.partition.parser.ASTEQ;
import com.netflix.metacat.common.server.partition.parser.ASTIN;
import com.netflix.metacat.common.server.partition.parser.ASTLIKE;
import com.netflix.metacat.common.server.partition.parser.ASTNOT;
import com.netflix.metacat.common.server.partition.parser.ASTNULL;
import com.netflix.metacat.common.server.partition.parser.ASTOR;
import com.netflix.metacat.common.server.partition.parser.ASTVAR;
import com.netflix.metacat.common.server.partition.parser.SimpleNode;
import com.netflix.metacat.common.server.partition.parser.Variable;
import com.netflix.metacat.common.server.partition.util.PartitionUtil;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Partition key evaluation.
 */
public class PartitionKeyParserEval extends PartitionParserEval {

    /**
     * Evaluate the expression.
     * @param node node in the expression tree
     * @param data data
     * @return Evaluated string
     */
    public String evalString(final SimpleNode node, final Object data) {
        final Object value1 = node.jjtGetChild(0).jjtAccept(this, data);
        final Compare comparison = (Compare) node.jjtGetChild(1).jjtAccept(this, data);
        final Object value2 = node.jjtGetChild(2).jjtAccept(this, data);
        if (comparison != Compare.EQ) {
            return null;
        }
        return String.format("%s=%s", value1, toValue(value2));
    }

    /**
     * Converts to String.
     * @param value value object
     * @return String
     */
    protected String toValue(final Object value) {
        return value == null ? PartitionUtil.DEFAULT_PARTITION_NAME : value.toString();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object visit(final ASTAND node, final Object data) {
        final Collection v1 = (Collection) node.jjtGetChild(0).jjtAccept(this, data);
        final Object b = node.jjtGetChild(1).jjtAccept(this, data);
        v1.addAll((Collection) b);
        return v1;
    }

    @Override
    public Object visit(final ASTEQ node, final Object data) {
        return Compare.EQ;
    }

    @Override
    public Object visit(final ASTCOMPARE node, final Object data) {
        Set<String> result = Sets.newHashSet();
        if (node.jjtGetNumChildren() == 3) {
            final String value = evalString(node, data);
            if (value != null) {
                result = Sets.newHashSet(value);
            }
        }
        return result;
    }

    @Override
    public Object visit(final ASTOR node, final Object data) {
        return new HashSet<String>();
    }

    @Override
    public Object visit(final ASTBETWEEN node, final Object data) {
        return new HashSet<String>();
    }

    @Override
    public Object visit(final ASTIN node, final Object data) {
        return new HashSet<String>();
    }

    @Override
    public Object visit(final ASTLIKE node, final Object data) {
        return new HashSet<String>();
    }

    @Override
    public Object visit(final ASTNOT node, final Object data) {
        return new HashSet<String>();
    }

    @Override
    public Object visit(final ASTNULL node, final Object data) {
        return new HashSet<String>();
    }

    @Override
    public Object visit(final ASTVAR node, final Object data) {
        return ((Variable) node.jjtGetValue()).getName();
    }

}
