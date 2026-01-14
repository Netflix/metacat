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

package com.netflix.metacat.common.server.util.hive

import com.google.common.collect.ImmutableMap
import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.server.connectors.model.PartitionListRequest
import com.netflix.metacat.common.server.partition.parser.PartitionParser
import com.netflix.metacat.common.server.connector.iceberg.IcebergTableCriteria
import com.netflix.metacat.common.server.connector.iceberg.IcebergTableHandler
import com.netflix.metacat.common.server.connector.iceberg.IcebergTableOpWrapper
import com.netflix.metacat.common.server.connector.iceberg.IcebergTableOpsProxy
import com.netflix.metacat.testdata.provider.DataDtoProvider
import com.netflix.spectator.api.NoopRegistry
import org.apache.iceberg.ScanSummary
import org.apache.iceberg.Schema
import org.apache.iceberg.Table
import org.apache.iceberg.expressions.Expression
import org.apache.iceberg.types.Type
import org.apache.iceberg.types.Types
import spock.lang.Specification

class IcebergFilterSpec extends Specification{


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
        'dateCreated>=150000'            | Expression.Operation.GT_EQ      | "ref(name=\"dateCreated\") >= 150000"
        'lastUpdated>=150000'            | Expression.Operation.GT_EQ      | "ref(name=\"lastUpdated\") >= 150000"
        'dateint<5 and dateCreated>=150000'   | Expression.Operation.AND      | "(ref(name=\"dateint\") < 5 and ref(name=\"dateCreated\") >= 150000)"
    }

    def 'Test iceberg filter throw exceptions' () {
        def dateint = Mock(Types.NestedField)
        def app = Mock(Types.NestedField)
        def type = Mock(Type)
        when:
        def fields = [dateint, app]
        final IcebergFilterGenerator generator = new IcebergFilterGenerator(fields);
        Expression expression = (Expression) new PartitionParser(new StringReader(filter)).filter()
            .jjtAccept(generator, null);
        then:
        thrown(RuntimeException)
        1 * dateint.name() >> "dateint"
        _ * dateint.type() >> type
        _ * type.typeId() >> Type.TypeID.INTEGER
        1 * app.name() >> "app"

        where:
        filter                           | resultop                     | resultstring
        'dateintcde<=8 and dateint>=5'   | Expression.Operation.AND     | "(ref(name=\"dateint\") <= 8 and ref(name=\"dateint\") >= 5)"
        'dateCreated_etest>=150000'      | Expression.Operation.GT_EQ      | "ref(name=\"dateCreated\") >= 150000"
    }

    def 'Test get icebergPartitionMap default iceberg summary fetch size' () {
        def icebergTable = Mock(Table)
        def icebergOpWrapper = Mock(IcebergTableOpWrapper)
        def icebergTableCriteria = Mock(IcebergTableCriteria)
        def partRequest = new PartitionListRequest()
        def registry = new NoopRegistry()
        def icebergUtil = new IcebergTableHandler(DataDtoProvider.newContext(null, ImmutableMap.of(HiveConfigConstants.ALLOW_RENAME_TABLE, "true")), icebergTableCriteria, icebergOpWrapper, new IcebergTableOpsProxy())


        when:
        icebergUtil.getIcebergTablePartitionMap(QualifiedName.fromString("tableName"), null, icebergTable)

        then:
        noExceptionThrown()
        icebergOpWrapper.getPartitionMetricsMap(_,_) >> ["result": Mock(ScanSummary.PartitionMetrics)]
    }

    def 'Test get icebergPartitionMap invoking filter' () {
        def icebergTableHelper = Mock(IcebergTableOpWrapper)
        def icebergTableCriteria = Mock(IcebergTableCriteria)
        def registry = new NoopRegistry()
        def icebergTable = Mock(Table)
        def schema = Mock(Schema)
        def dateint = Mock(Types.NestedField)
        def type = Mock(Type)
        def app = Mock(Types.NestedField)
        def icebergUtil = new IcebergTableHandler(DataDtoProvider.newContext(null, ImmutableMap.of(HiveConfigConstants.ALLOW_RENAME_TABLE, "true")), icebergTableCriteria, icebergTableHelper, new IcebergTableOpsProxy())

        when:
        icebergUtil.getIcebergTablePartitionMap(QualifiedName.fromString("tableName"), "dateint==1", icebergTable)

        then:
        noExceptionThrown()
        icebergTableHelper.getPartitionMetricsMap(_,_) >> ["result": Mock(ScanSummary.PartitionMetrics)]
        icebergTable.schema() >> schema
        schema.columns() >> [dateint, app]
        dateint.name() >> "dateint"
        dateint.type() >> type
        type.typeId() >> Type.TypeID.INTEGER
        app.name() >> "app"
    }

}
