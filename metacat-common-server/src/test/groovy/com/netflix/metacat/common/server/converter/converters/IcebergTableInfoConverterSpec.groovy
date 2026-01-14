/*
 *  Copyright 2017 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */

package com.netflix.metacat.common.server.converter.converters

import org.apache.iceberg.PartitionField
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.Schema
import org.apache.iceberg.transforms.Identity
import org.apache.iceberg.types.Type
import org.apache.iceberg.types.Types
import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.server.connectors.model.TableInfo
import com.netflix.metacat.common.server.connector.iceberg.IcebergTableWrapper
import com.netflix.metacat.common.server.connector.sql.DirectSqlTable
import spock.lang.Specification

/**
 * Unit test for IcebergTableInfoConverter.
 *
 * @author metacat team
 * @since 1.4.0
 */
class IcebergTableInfoConverterSpec extends Specification {
    IcebergTableInfoConverter converter

    def setup() {
        converter = new IcebergTableInfoConverter()
    }

    def "test fromIcebergTableToTableInfo"() {
        def icebergTable = Mock(org.apache.iceberg.Table)
        def icebergTableWrapper = new IcebergTableWrapper(icebergTable, [:])
        def partSpec = Mock(PartitionSpec)
        def field = Mock(PartitionField)
        def schema =  Mock(Schema)
        def nestedField = Mock(Types.NestedField)
        def nestedField2 = Mock(Types.NestedField)
        def type = Mock(Type)
        when:
        def tableInfo = converter.fromIcebergTableToTableInfo(QualifiedName.ofTable('c', 'd', 't'),
            icebergTableWrapper, "/tmp/test", TableInfo.builder().build() )
        then:
        2 * field.transform() >> Mock(Identity)
        1 * icebergTable.properties() >> ["test":"abd"]
        2 * icebergTable.spec() >>  partSpec
        2 * icebergTable.refs() >> ["main": Mock(org.apache.iceberg.SnapshotRef) {
            isBranch() >> true
            isTag() >> false
        }]
        1 * partSpec.fields() >> [ field]
        1 * icebergTable.schema() >> schema
        1 * schema.columns() >> [nestedField, nestedField2]
        3 * nestedField.name() >> "fieldName"
        1 * nestedField.doc() >> "fieldName doc"
        2 * nestedField.type() >> type
        1 * nestedField.isOptional() >> true
        2 * nestedField2.name() >> "fieldName2"
        1 * nestedField2.doc() >> "fieldName2 doc"
        2 * nestedField2.type() >> type
        1 * nestedField2.isOptional() >> true
        2 * type.typeId() >> Type.TypeID.BOOLEAN
        _ * type.toString() >> "boolean"
        1 * schema.findField(_) >> nestedField

        tableInfo.getMetadata().get(DirectSqlTable.PARAM_TABLE_TYPE).equals(DirectSqlTable.ICEBERG_TABLE_TYPE)
        tableInfo.getMetadata().get(DirectSqlTable.PARAM_METADATA_LOCATION).equals("/tmp/test")
        tableInfo.getFields().size() == 2
        tableInfo.getFields().get(0).isPartitionKey() != tableInfo.getFields().get(1).isPartitionKey()
        tableInfo.getFields().get(0).getComment() == 'fieldName doc'

    }

    def "test fromIcebergTableToTableInfo includes branch/tag metadata"() {
        def icebergTable = Mock(org.apache.iceberg.Table)
        def icebergTableWrapper = Mock(IcebergTableWrapper)
        when:
        def tableInfo = converter.fromIcebergTableToTableInfo(QualifiedName.ofTable('c', 'd', 't'),
            icebergTableWrapper, "/tmp/test", TableInfo.builder().build() )
        then:
        1 * icebergTableWrapper.getTable() >> icebergTable
        1 * icebergTableWrapper.populateBranchTagMetadata() >> [
            "iceberg.has.non.main.branches": "true",
            "iceberg.has.tags": "false"
        ] // Called by converter and returns metadata map
        1 * icebergTableWrapper.getExtraProperties() >> [:] // Called by converter for other properties
        1 * icebergTable.properties() >> [:]
        1 * icebergTable.schema() >> Mock(Schema) {
            columns() >> []
        }
        2 * icebergTable.spec() >> Mock(PartitionSpec) {
            fields() >> []
            toString() >> "[]"
        }

        // Verify that non-main-branch/tag metadata is injected via extraProperties
        tableInfo.getMetadata().get("iceberg.has.non.main.branches") == "true"
        tableInfo.getMetadata().get("iceberg.has.tags") == "false"
    }
}
