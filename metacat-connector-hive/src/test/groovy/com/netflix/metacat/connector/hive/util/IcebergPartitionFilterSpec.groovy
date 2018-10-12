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

package com.netflix.metacat.connector.hive.util

import com.netflix.iceberg.expressions.Expression
import com.netflix.iceberg.types.Type
import com.netflix.iceberg.types.Types
import com.netflix.metacat.common.server.partition.parser.PartitionParser
import spock.lang.Specification

class IcebergPartitionFilterSpec extends Specification{

    def 'Test iceberg filter' () {
        def dateint = Mock(Types.NestedField)
        def app = Mock(Types.NestedField)
        def type = Mock(Type)
        when:
        def fields = [dateint, app]
        final IcebergFilterGenerator generator = new IcebergFilterGenerator(fields);
        Expression expression = (Expression) new PartitionParser(new StringReader(filter)).filter()
            .jjtAccept(generator, null);
        then:
        expression.op().equals(resultop)
        expression.toString().equals(resultstring.toString())
        1 * dateint.name() >> "dateint"
        _ * dateint.type() >> type
        _ * type.typeId() >> Type.TypeID.INTEGER
        1 * app.name() >> "app"


        where:
        filter                           | resultop                     | resultstring
        'dateint<=8 and dateint>=5'      | Expression.Operation.AND     | "(ref(name=\"dateint\") <= 8 and ref(name=\"dateint\") >= 5)"
        'dateint not between 5 and 8'    | Expression.Operation.OR      | "(ref(name=\"dateint\") < 5 or ref(name=\"dateint\") > 8)"
        'dateint between 5 and 8'        | Expression.Operation.AND     | "(ref(name=\"dateint\") >= 5 and ref(name=\"dateint\") <= 8)"
        'dateint==5'                     | Expression.Operation.EQ      | "ref(name=\"dateint\") == 5"
        'dateint!=5'                     | Expression.Operation.NOT_EQ      | "ref(name=\"dateint\") != 5"
        'dateint>=5'                     | Expression.Operation.GT_EQ      | "ref(name=\"dateint\") >= 5"
        'dateint>5'                      | Expression.Operation.GT      | "ref(name=\"dateint\") > 5"
        'dateint<=5'                     | Expression.Operation.LT_EQ      | "ref(name=\"dateint\") <= 5"
        'dateint<5'                      | Expression.Operation.LT      | "ref(name=\"dateint\") < 5"
        'dateint<>5'                     | Expression.Operation.NOT_EQ      | "ref(name=\"dateint\") != 5"
        'app==\"abc\"'                   | Expression.Operation.EQ      | "ref(name=\"app\") == \"abc\""
        'app!=\"abc\"'                   | Expression.Operation.NOT_EQ      | "ref(name=\"app\") != \"abc\""
        'app>=\"abc\"'                   | Expression.Operation.GT_EQ      | "ref(name=\"app\") >= \"abc\""
    }

}
