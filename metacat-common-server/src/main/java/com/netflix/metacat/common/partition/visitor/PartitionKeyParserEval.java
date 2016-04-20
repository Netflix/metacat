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

import com.google.common.collect.Sets;
import com.netflix.metacat.common.partition.parser.ASTAND;
import com.netflix.metacat.common.partition.parser.ASTBETWEEN;
import com.netflix.metacat.common.partition.parser.ASTCOMPARE;
import com.netflix.metacat.common.partition.parser.ASTEQ;
import com.netflix.metacat.common.partition.parser.ASTIN;
import com.netflix.metacat.common.partition.parser.ASTLIKE;
import com.netflix.metacat.common.partition.parser.ASTNOT;
import com.netflix.metacat.common.partition.parser.ASTOR;
import com.netflix.metacat.common.partition.parser.ASTVAR;
import com.netflix.metacat.common.partition.parser.SimpleNode;
import com.netflix.metacat.common.partition.parser.Variable;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class PartitionKeyParserEval extends PartitionParserEval {

    public String evalString(SimpleNode node, Object data) {
        Object value1 = node.jjtGetChild(0).jjtAccept(this, data);
        Compare comparison = (Compare) node.jjtGetChild(1).jjtAccept(this, data);
        Object value2 = node.jjtGetChild(2).jjtAccept(this, data);
        if (comparison != Compare.EQ) {
            return null;
        }
        return String.format("%s=%s", value1, value2.toString());
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object visit(ASTAND node, Object data) {
        Collection v1 = (Collection) node.jjtGetChild(0).jjtAccept(this, data);
        Object b = node.jjtGetChild(1).jjtAccept(this, data);
        v1.addAll((Collection) b);
        return v1;
    }

    @Override
    public Object visit(ASTEQ node, Object data) {
        return Compare.EQ;
    }

    @Override
    public Object visit(ASTCOMPARE node, Object data) {
        Set<String> result = Sets.newHashSet();
        String value = evalString(node, data);
        if( value != null){
            result = Sets.newHashSet(value);
        }
        return result;
    }

    @Override
    public Object visit(ASTOR node, Object data) {
        return new HashSet<String>();
    }

    @Override
    public Object visit(ASTBETWEEN node, Object data) {
        return new HashSet<String>();
    }

    @Override
    public Object visit(ASTIN node, Object data) {
        return new HashSet<String>();
    }

    @Override
    public Object visit(ASTLIKE node, Object data) {
        return new HashSet<String>();
    }

    @Override
    public Object visit(ASTNOT node, Object data) {
        return new HashSet<String>();
    }

    @Override
    public Object visit(ASTVAR node, Object data) {
        return  ((Variable)node.jjtGetValue()).getName();
    }

}
