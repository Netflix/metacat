/*
 *
 *  Copyright 2016 Netflix, Inc.
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
 *
 */
package com.netflix.metacat.common.dto

import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.json.MetacatJson
import com.netflix.metacat.common.json.MetacatJsonLocator
import spock.lang.Specification
import spock.lang.Unroll

class TableDtoSpec extends Specification {

    static final String KEY = TableDto.CURRENT_SNAPSHOT_ID_METADATA_KEY

    @Unroll
    def "getCurrentSnapshotId: #scenario"() {
        expect:
        new TableDto(metadata: metadata).getCurrentSnapshotId() == expected

        where:
        scenario                      | metadata                       || expected
        "present positive id"         | [(KEY): '5186921321658503645'] || Optional.of(5186921321658503645L)
        "present -1 (no snapshot)"    | [(KEY): '-1']                  || Optional.of(-1L)
        "key absent (non-iceberg)"    | ['metadata_location': 's3']    || Optional.empty()
        "null metadata map"           | null                           || Optional.empty()
        "empty string value"          | [(KEY): '']                    || Optional.empty()
        "blank value"                 | [(KEY): '   ']                 || Optional.empty()
        "non-numeric value"           | [(KEY): 'abc']                 || Optional.empty()
    }

    @Unroll
    def "current_snapshot_id (#value) survives a MetacatJson TableDto -> JSON -> TableDto round-trip"() {
        given:
        MetacatJson metacatJson = new MetacatJsonLocator()
        def table = new TableDto(
            name: QualifiedName.ofTable('c', 'd', 't'),
            metadata: [(KEY): value, 'metadata_location': 's3://x/00002.metadata.json'])

        when:
        def json = metacatJson.toJsonString(table)
        def restored = metacatJson.parseJsonValue(json, TableDto.class)

        then:
        json.contains('"' + KEY + '":"' + value + '"')
        restored.getMetadata().get(KEY) == value
        restored.getMetadata().get('metadata_location') == 's3://x/00002.metadata.json'
        restored.getCurrentSnapshotId() == expected

        where:
        value                   || expected
        '5186921321658503645'   || Optional.of(5186921321658503645L)
        '-1'                    || Optional.of(-1L)
    }
}
