/*
 * Copyright 2024 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.metacat.connector.hive.iceberg

import org.apache.iceberg.Table
import org.apache.iceberg.SnapshotRef
import spock.lang.Specification

/**
 * Tests for IcebergTableWrapper branch and tag detection functionality.
 * These tests verify the Iceberg 1.9 native API integration for identifying
 * branches and tags in tables.
 */
class IcebergTableWrapperSpec extends Specification {

    def "test table with no refs"() {
        given: "A table with no references"
        def mockTable = Mock(Table) {
            refs() >> [:]
        }
        def extraProperties = [:]

        when: "Creating wrapper"
        def wrapper = new IcebergTableWrapper(mockTable, extraProperties)

        then: "Should detect no tags"
        !wrapper.hasTags()
    }

    def "test table with only main branch"() {
        given: "A table with only the main branch"
        def mockBranch = Mock(SnapshotRef) {
            isBranch() >> true
            isTag() >> false
        }
        def mockTable = Mock(Table) {
            refs() >> ["main": mockBranch]
        }
        def extraProperties = [:]

        when: "Creating wrapper"
        def wrapper = new IcebergTableWrapper(mockTable, extraProperties)

        then: "Should detect main branch"
        !wrapper.hasTags()
    }

    def "test table with multiple branches including main"() {
        given: "A table with main and additional branches"
        def mockBranch = Mock(SnapshotRef) {
            isBranch() >> true
            isTag() >> false
        }
        def mockTable = Mock(Table) {
            refs() >> ["main": mockBranch, "feature-branch": mockBranch, "dev-branch": mockBranch]
        }
        def extraProperties = [:]

        when: "Creating wrapper"
        def wrapper = new IcebergTableWrapper(mockTable, extraProperties)

        then: "Should detect multiple branches"
        !wrapper.hasTags()
    }

    def "test table with only tags"() {
        given: "A table with only tags"
        def mockTag = Mock(SnapshotRef) {
            isBranch() >> false
            isTag() >> true
        }
        def mockTable = Mock(Table) {
            refs() >> ["v1.0": mockTag, "v1.1": mockTag, "release-2024": mockTag]
        }
        def extraProperties = [:]

        when: "Creating wrapper"
        def wrapper = new IcebergTableWrapper(mockTable, extraProperties)

        then: "Should detect tags only"
        wrapper.hasTags()
    }

    def "test table with branches and tags"() {
        given: "A table with both branches and tags"
        def mockBranch = Mock(SnapshotRef) {
            isBranch() >> true
            isTag() >> false
        }
        def mockTag = Mock(SnapshotRef) {
            isBranch() >> false
            isTag() >> true
        }
        def mockTable = Mock(Table) {
            refs() >> [
                "main": mockBranch,
                "feature-x": mockBranch, 
                "v1.0": mockTag,
                "v2.0": mockTag
            ]
        }
        def extraProperties = [:]

        when: "Creating wrapper"
        def wrapper = new IcebergTableWrapper(mockTable, extraProperties)

        then: "Should detect both branches and tags"
        wrapper.hasTags()
    }

    def "test table with main and feature branch"() {
        given: "A table with main branch plus a feature branch"
        def mockBranch = Mock(SnapshotRef) {
            isBranch() >> true
            isTag() >> false
        }
        def mockTable = Mock(Table) {
            refs() >> ["main": mockBranch, "feature-only": mockBranch]
        }
        def extraProperties = [:]

        when: "Creating wrapper"
        def wrapper = new IcebergTableWrapper(mockTable, extraProperties)

        then: "Should detect multiple branches"
        !wrapper.hasTags()
    }

    def "test table created with Iceberg client < 0.14.1 (has main branch only)"() {
        given: "A table created with Iceberg client < 0.14.1 (Iceberg auto-creates main branch)"
        def mockMainBranch = Mock(SnapshotRef) {
            isBranch() >> true
            isTag() >> false
        }
        def mockTable = Mock(Table) {
            refs() >> ["main": mockMainBranch]
        }
        def extraProperties = [:]

        when: "Creating wrapper"
        def wrapper = new IcebergTableWrapper(mockTable, extraProperties)

        then: "Should detect main branch but no tags"
        !wrapper.hasTags()

        when: "Populating metadata for pre-0.14.1 client table"
        def metadataMap = wrapper.populateBranchTagMetadata()

        then: "Should return correct values"
        noExceptionThrown()
        metadataMap.get(IcebergTableWrapper.ICEBERG_HAS_TAGS_KEY) == "false"
        !extraProperties.containsKey(IcebergTableWrapper.ICEBERG_HAS_TAGS_KEY)
    }

    def "test static constants and separate key population"() {
        given: "A table with branches and tags"
        def mockBranch = Mock(SnapshotRef) {
            isBranch() >> true
            isTag() >> false
        }
        def mockTag = Mock(SnapshotRef) {
            isBranch() >> false
            isTag() >> true
        }
        def mockTable = Mock(Table) {
            refs() >> ["main": mockBranch, "feature": mockBranch, "v1.0": mockTag]
        }
        def extraProperties = [:]

        when: "Creating wrapper and populating metadata"
        def wrapper = new IcebergTableWrapper(mockTable, extraProperties)
        def metadataMap = wrapper.populateBranchTagMetadata()

        then: "Should return the tags key correctly"
        metadataMap.get(IcebergTableWrapper.ICEBERG_HAS_TAGS_KEY) == "true"
        !extraProperties.containsKey(IcebergTableWrapper.ICEBERG_HAS_TAGS_KEY)

        and: "Static constant should have expected value"
        IcebergTableWrapper.ICEBERG_HAS_TAGS_KEY == "iceberg.has.tags"
    }

    def "test separate key population - branches only"() {
        given: "A table with only branches"
        def mockBranch = Mock(SnapshotRef) {
            isBranch() >> true
            isTag() >> false
        }
        def mockTable = Mock(Table) {
            refs() >> ["main": mockBranch, "feature": mockBranch]
        }
        def extraProperties = [:]

        when: "Creating wrapper and populating metadata"
        def wrapper = new IcebergTableWrapper(mockTable, extraProperties)
        def metadataMap = wrapper.populateBranchTagMetadata()

        then: "Should return tags=false"
        metadataMap.get(IcebergTableWrapper.ICEBERG_HAS_TAGS_KEY) == "false"
        !extraProperties.containsKey(IcebergTableWrapper.ICEBERG_HAS_TAGS_KEY)
    }

    def "test separate key population - tags only"() {
        given: "A table with only tags"
        def mockTag = Mock(SnapshotRef) {
            isBranch() >> false
            isTag() >> true
        }
        def mockTable = Mock(Table) {
            refs() >> ["v1.0": mockTag, "v2.0": mockTag]
        }
        def extraProperties = [:]

        when: "Creating wrapper and populating metadata"
        def wrapper = new IcebergTableWrapper(mockTable, extraProperties)
        def metadataMap = wrapper.populateBranchTagMetadata()

        then: "Should return tags=true"
        metadataMap.get(IcebergTableWrapper.ICEBERG_HAS_TAGS_KEY) == "true"
        !extraProperties.containsKey(IcebergTableWrapper.ICEBERG_HAS_TAGS_KEY)
    }

    def "test separate key population - neither"() {
        given: "A table with only main branch"
        def mockBranch = Mock(SnapshotRef) {
            isBranch() >> true
            isTag() >> false
        }
        def mockTable = Mock(Table) {
            refs() >> ["main": mockBranch]
        }
        def extraProperties = [:]

        when: "Creating wrapper and populating metadata"
        def wrapper = new IcebergTableWrapper(mockTable, extraProperties)
        def metadataMap = wrapper.populateBranchTagMetadata()

        then: "Should return tags=false"
        metadataMap.get(IcebergTableWrapper.ICEBERG_HAS_TAGS_KEY) == "false"
        !extraProperties.containsKey(IcebergTableWrapper.ICEBERG_HAS_TAGS_KEY)
    }

    def "test constructor does not populate metadata automatically"() {
        given: "A table with branches and tags"
        def mockBranch = Mock(SnapshotRef) {
            isBranch() >> true
            isTag() >> false
        }
        def mockTag = Mock(SnapshotRef) {
            isBranch() >> false
            isTag() >> true
        }
        def mockTable = Mock(Table) {
            refs() >> ["main": mockBranch, "feature": mockBranch, "v1.0": mockTag]
        }
        def extraProperties = [:]

        when: "Creating wrapper without explicit metadata population"
        def wrapper = new IcebergTableWrapper(mockTable, extraProperties)

        then: "Should NOT automatically populate metadata keys"
        !extraProperties.containsKey(IcebergTableWrapper.ICEBERG_HAS_TAGS_KEY)

        and: "Methods should still work"
        wrapper.hasTags()
    }

    def "test legacy constructor compatibility"() {
        given: "Using the legacy constructor pattern"
        def mockBranch = Mock(SnapshotRef) {
            isBranch() >> true
            isTag() >> false
        }
        def mockTag = Mock(SnapshotRef) {
            isBranch() >> false
            isTag() >> true
        }
        def mockTable = Mock(Table) {
            refs() >> ["main": mockBranch, "feature": mockBranch, "v1.0": mockTag]
        }
        def extraProperties = ["metadata_content": "some-content"]

        when: "Creating wrapper"
        def wrapper = new IcebergTableWrapper(mockTable, extraProperties)

        then: "Should work correctly and detect tags"
        wrapper.getTable() == mockTable
        wrapper.getExtraProperties() == extraProperties
        wrapper.hasTags()
    }

    def "test mixed reference types"() {
        given: "A table with unknown reference type mixed with known ones"
        def mockBranch = Mock(SnapshotRef) {
            isBranch() >> true
            isTag() >> false
        }
        def mockTag = Mock(SnapshotRef) {
            isBranch() >> false
            isTag() >> true
        }
        def mockUnknown = Mock(SnapshotRef) {
            isBranch() >> false
            isTag() >> false  // Unknown type is neither branch nor tag
        }
        def mockTable = Mock(Table) {
            refs() >> [
                "main": mockBranch,
                "v1.0": mockTag, 
                "unknown": mockUnknown  // This should be ignored
            ]
        }
        def extraProperties = [:]

        when: "Creating wrapper"
        def wrapper = new IcebergTableWrapper(mockTable, extraProperties)

        then: "Should only detect known reference types"
        wrapper.hasTags()
    }

    def "test empty string reference type"() {
        given: "A table with empty string reference type"
        def mockBranch = Mock(SnapshotRef) {
            isBranch() >> true
            isTag() >> false
        }
        def mockEmpty = Mock(SnapshotRef) {
            isBranch() >> false
            isTag() >> false  // Empty type is neither branch nor tag
        }
        def mockTable = Mock(Table) {
            refs() >> [
                "main": mockBranch,
                "empty-type": mockEmpty
            ]
        }
        def extraProperties = [:]

        when: "Creating wrapper"
        def wrapper = new IcebergTableWrapper(mockTable, extraProperties)

        then: "Should only detect valid reference types"
        !wrapper.hasTags()
    }
}
