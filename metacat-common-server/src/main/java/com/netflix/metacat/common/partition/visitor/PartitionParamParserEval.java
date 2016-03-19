package com.netflix.metacat.common.partition.visitor;

import com.netflix.metacat.common.partition.parser.SimpleNode;

public class PartitionParamParserEval extends PartitionKeyParserEval {

    public String evalString(SimpleNode node, Object data) {
        Object value1 = node.jjtGetChild(0).jjtAccept(this, data);
        if (!"dateCreated".equals(value1)) {
            return null;
        }
        Compare comparison = (Compare) node.jjtGetChild(1).jjtAccept(this, data);
        Object value2 = node.jjtGetChild(2).jjtAccept(this, data);
        return String.format("%s%s%s", value1, comparison.getExpression(),value2.toString());
    }
}
