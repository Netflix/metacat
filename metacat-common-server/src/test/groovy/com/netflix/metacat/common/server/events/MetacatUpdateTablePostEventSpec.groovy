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
package com.netflix.metacat.common.server.events

import com.netflix.metacat.common.MetacatRequestContext
import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.dto.TableDto
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Specification for the derived metadata-only indicator on MetacatUpdateTablePostEvent, and for the
 * TableDto.getCurrentSnapshotId() helper it relies on.
 */
class MetacatUpdateTablePostEventSpec extends Specification {

    static final String KEY = TableDto.CURRENT_SNAPSHOT_ID_METADATA_KEY
    def qName = QualifiedName.ofTable('c', 'd', 't')
    def ctx = MetacatRequestContext.builder().build()

    private static TableDto tableWithSnapshot(String snapshotId) {
        // snapshotId == null models a non-Iceberg table (the key is simply absent from the metadata map)
        return new TableDto(
            name: QualifiedName.ofTable('c', 'd', 't'),
            metadata: snapshotId == null ? ['metadata_location': 's3://x'] : [(KEY): snapshotId]
        )
    }

    @Unroll
    def "getCurrentSnapshotId: #scenario"() {
        expect:
        new TableDto(metadata: metadata).getCurrentSnapshotId() == expected

        where:
        scenario                      | metadata                    || expected
        "present positive id"         | [(KEY): '5186921321658503645'] || Optional.of(5186921321658503645L)
        "present -1 (no snapshot)"    | [(KEY): '-1']               || Optional.of(-1L)
        "key absent (non-iceberg)"    | ['metadata_location': 's3'] || Optional.empty()
        "null metadata map"           | null                        || Optional.empty()
        "empty string value"         | [(KEY): '']                 || Optional.empty()
        "blank value"                 | [(KEY): '   ']              || Optional.empty()
        "non-numeric value"           | [(KEY): 'abc']              || Optional.empty()
    }

    @Unroll
    def "getMetadataOnlyUpdate is #expected when #scenario"() {
        given:
        def event = new MetacatUpdateTablePostEvent(
            qName, ctx, this, tableWithSnapshot(oldId), tableWithSnapshot(newId), latest)

        expect:
        event.getMetadataOnlyUpdate() == expected

        where:
        scenario                                          | oldId  | newId  | latest || expected
        "iceberg snapshot unchanged"                      | '100'  | '100'  | true   || true
        "iceberg snapshot changed (data write)"           | '100'  | '200'  | true   || false
        "empty iceberg table, metadata-only change"       | '-1'   | '-1'   | true   || true
        "empty iceberg table gets first data write"       | '-1'   | '100'  | true   || false
        "non-iceberg table on both sides"                 | null   | null   | true   || false
        "snapshot id missing on the current side"         | '100'  | null   | true   || false
        "snapshot id missing on the old side"             | null   | '100'  | true   || false
        "current table could not be refreshed (stale)"    | '100'  | '100'  | false  || false
    }

    def "getMetadataOnlyUpdate defaults to latest-current-table via the 5-arg constructor"() {
        given:
        def event = new MetacatUpdateTablePostEvent(
            qName, ctx, this, tableWithSnapshot('7'), tableWithSnapshot('7'))

        expect:
        event.isLatestCurrentTable()
        event.getMetadataOnlyUpdate()
    }
}
