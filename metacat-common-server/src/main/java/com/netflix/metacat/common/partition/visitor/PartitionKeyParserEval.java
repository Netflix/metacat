package com.netflix.metacat.common.partition.visitor;

import com.google.common.collect.Sets;
import com.netflix.metacat.common.partition.parser.ASTAND;
import com.netflix.metacat.common.partition.parser.ASTEQ;
import com.netflix.metacat.common.partition.parser.ASTEVAL;
import com.netflix.metacat.common.partition.parser.ASTFILTER;
import com.netflix.metacat.common.partition.parser.ASTGT;
import com.netflix.metacat.common.partition.parser.ASTGTE;
import com.netflix.metacat.common.partition.parser.ASTLIKE;
import com.netflix.metacat.common.partition.parser.ASTLT;
import com.netflix.metacat.common.partition.parser.ASTLTE;
import com.netflix.metacat.common.partition.parser.ASTMATCHES;
import com.netflix.metacat.common.partition.parser.ASTNEQ;
import com.netflix.metacat.common.partition.parser.ASTNEVAL;
import com.netflix.metacat.common.partition.parser.ASTNUM;
import com.netflix.metacat.common.partition.parser.ASTOR;
import com.netflix.metacat.common.partition.parser.ASTSTRING;
import com.netflix.metacat.common.partition.parser.ASTVAR;
import com.netflix.metacat.common.partition.parser.PartitionParserVisitor;
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
    public Object visit(ASTEVAL node, Object data) {
        Set<String> result = Sets.newHashSet();
        String value = evalString(node, data);
        if( value != null){
            result = Sets.newHashSet(value);
        }
        return result;
    }

    @Override
    public Object visit(ASTNEVAL node, Object data) {
        return new HashSet<String>();
    }

    @Override
    public Object visit(ASTOR node, Object data) {
        return new HashSet<String>();
    }

    @Override
    public Object visit(ASTVAR node, Object data) {
        return  ((Variable)node.jjtGetValue()).getName();
    }

}
