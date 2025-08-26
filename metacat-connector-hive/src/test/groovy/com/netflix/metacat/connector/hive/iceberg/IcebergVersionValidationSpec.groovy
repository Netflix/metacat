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

import org.apache.iceberg.*
import org.apache.iceberg.types.Types
import spock.lang.Specification

/**
 * Simple validation tests to ensure Iceberg 1.1.0 upgrade is working correctly.
 * These tests focus on API compatibility and version verification.
 */
class IcebergVersionValidationSpec extends Specification {

    def "test Iceberg 1.1.0 API classes are available"() {
        expect: "All critical Iceberg classes should be available"
        Class.forName('org.apache.iceberg.ScanSummary') != null
        Class.forName('org.apache.iceberg.Table') != null
        Class.forName('org.apache.iceberg.TableMetadataParser') != null
        Class.forName('org.apache.iceberg.UpdateSchema') != null
        Class.forName('org.apache.iceberg.BaseMetastoreTableOperations') != null
        Class.forName('org.apache.iceberg.hadoop.HadoopFileIO') != null
    }

    def "test ScanSummary.PartitionMetrics constructor works"() {
        when: "Create PartitionMetrics using Iceberg 1.1.0 constructor"
        def metrics = new ScanSummary.PartitionMetrics(
            fileCount: 10,
            recordCount: 1000,
            dataTimestampMillis: System.currentTimeMillis()
        )
        
        then: "Should create successfully"
        metrics != null
        metrics.fileCount() == 10
        metrics.recordCount() == 1000
        metrics.dataTimestampMillis() != null
    }

    def "test Schema creation and field access works"() {
        when: "Create Iceberg schema using 1.1.0 API"
        def schema = new Schema([
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get(), "Test comment"),
            Types.NestedField.optional(3, "value", Types.DoubleType.get())
        ])
        
        then: "Schema should be created correctly"
        schema != null
        schema.columns().size() == 3
        schema.findField("name").doc() == "Test comment"
        schema.findField("id").isRequired() == true
        schema.findField("name").isOptional() == true
    }

    def "test TableMetadataParser API exists"() {
        expect: "TableMetadataParser.toJson method should exist"
        def method = TableMetadataParser.class.getDeclaredMethod('toJson', TableMetadata.class)
        method != null
        method.getReturnType() == String.class
    }

    def "test UpdateSchema API exists"() {
        expect: "UpdateSchema should have expected methods"
        def updateColumnDocMethod = UpdateSchema.class.getDeclaredMethod('updateColumnDoc', String.class, String.class)
        updateColumnDocMethod != null
        
        // Check if commit method exists (might be inherited or have different signature)
        def commitMethods = UpdateSchema.class.getMethods().findAll { it.name == 'commit' }
        commitMethods.size() > 0
    }

    def "test BaseMetastoreTableOperations methods exist"() {
        expect: "BaseMetastoreTableOperations should have expected methods"
        def ioMethods = BaseMetastoreTableOperations.class.getMethods().findAll { it.name == 'io' }
        ioMethods.size() > 0
        
        def currentMethods = BaseMetastoreTableOperations.class.getMethods().findAll { it.name == 'current' }
        currentMethods.size() > 0
        currentMethods.any { it.returnType == TableMetadata.class }
    }

    def "test version detection"() {
        when: "Check Iceberg version information"
        def icebergVersion = getIcebergVersionInfo()
        
        then: "Should indicate we're using version 1.x"
        println "Iceberg version info: ${icebergVersion}"
        icebergVersion.contains("1.") || icebergVersion.contains("post-0.13")
        !icebergVersion.contains("0.13.1") // Should not be the old version
    }

    private String getIcebergVersionInfo() {
        try {
            // Try to get version from package
            def packageInfo = Package.getPackage("org.apache.iceberg")
            if (packageInfo?.implementationVersion) {
                return "Package version: ${packageInfo.implementationVersion}"
            }
            
            // Check for new 1.1+ features that didn't exist in 0.13
            try {
                // This class exists in Iceberg 1.0+
                Class.forName("org.apache.iceberg.actions.RewriteDataFiles")
                return "1.0+ (has RewriteDataFiles)"
            } catch (ClassNotFoundException e) {
                // This means we're on an older version
                return "pre-1.0"
            }
        } catch (Exception e) {
            return "unknown: ${e.message}"
        }
    }
}
