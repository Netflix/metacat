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
