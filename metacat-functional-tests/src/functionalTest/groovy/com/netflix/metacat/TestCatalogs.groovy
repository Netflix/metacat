/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.netflix.metacat

import com.netflix.metacat.common.QualifiedName
import groovy.transform.Canonical

class TestCatalogs {
    @Canonical
    static class TestCatalog {
        boolean createDatabase
        boolean createPartition
        boolean createTable
        boolean deleteDatabase
        boolean deleteTable
        boolean createView
        List<QualifiedName> createdDatabases = []
        List<QualifiedName> createdPartitions = []
        List<QualifiedName> createdTables = []
        String name
        boolean partitionKeysAppearLast
        List<QualifiedName> preExistingDatabases = []
        List<QualifiedName> preExistingTables = []
        String thriftUri
        String type
        boolean validateFilterExpressionBasedOnPartitionKeyType = true
    }

    static final Set<TestCatalog> ALL = [
            new TestCatalog(
                    createDatabase: true,
                    createPartition: true,
                    createTable: true,
                    deleteDatabase: true,
                    deleteTable: true,
                    name: 'hive-metastore',
                    partitionKeysAppearLast: true,
                    type: 'hive',
                    createView: true,
            ),
            new TestCatalog(
                    createDatabase: true,
                    createPartition: true,
                    createTable: true,
                    deleteDatabase: true,
                    deleteTable: true,
                    name: 's3-mysql-db',
                    type: 's3',
                    validateFilterExpressionBasedOnPartitionKeyType: false
            ),
//            new TestCatalog(
//                    name: 'mysql-56-db',
//                    preExistingDatabases: [
//                            QualifiedName.ofDatabase('mysql-56-db', 'sakila'),
//                            QualifiedName.ofDatabase('mysql-56-db', 'world'),
//                    ],
//                    preExistingTables: [
//                            QualifiedName.ofTable('mysql-56-db', 'sakila', 'city'),
//                            QualifiedName.ofTable('mysql-56-db', 'sakila', 'country'),
//                            QualifiedName.ofTable('mysql-56-db', 'world', 'City'),
//                            QualifiedName.ofTable('mysql-56-db', 'world', 'Country'),
//                    ],
//                    type: 'metacat-mysql',
//            ),
    ]

    static TestCatalog findByCatalogName(String name) {
        TestCatalog catalog = ALL.find { it.name == name }
        assert catalog, "Unable to locate catalog $name"
        return catalog
    }

    static TestCatalog findByQualifiedName(QualifiedName name) {
        TestCatalog catalog = ALL.find { it.name == name.catalogName }
        assert catalog, "Unable to locate catalog $name"
        return catalog
    }

    static Collection<QualifiedName> getAllDatabases(Collection<TestCatalog> source) {
        return source.collect { it.createdDatabases + it.preExistingDatabases }.flatten() as Collection<QualifiedName>
    }

    static Collection<QualifiedName> getCreatedDatabases(Collection<TestCatalog> source) {
        return source.collect { it.createdDatabases }.flatten() as Collection<QualifiedName>
    }

    static Collection<QualifiedName> getPreExistingDatabases(Collection<TestCatalog> source) {
        return source.collect { it.preExistingDatabases }.flatten() as Collection<QualifiedName>
    }

    static Collection<QualifiedName> getAllTables(Collection<TestCatalog> source) {
        return source.collect { it.createdTables + it.preExistingTables }.flatten() as Collection<QualifiedName>
    }

    static Collection<QualifiedName> getCreatedTables(Collection<TestCatalog> source) {
        return source.collect { it.createdTables }.flatten() as Collection<QualifiedName>
    }

    static Collection<QualifiedName> getPreExistingTables(Collection<TestCatalog> source) {
        return source.collect { it.preExistingTables }.flatten() as Collection<QualifiedName>
    }

    static Collection<QualifiedName> getCreatedPartitions(Collection<TestCatalog> source) {
        return source.collect { it.createdPartitions }.flatten() as Collection<QualifiedName>
    }

    static Collection<TestCatalog> getThriftImplementers(Collection<TestCatalog> source) {
        return source.findAll { it.thriftUri }
    }

    static Collection<TestCatalog> getCanCreateDatabase(Collection<TestCatalog> source) {
        return source.findAll { it.createDatabase }
    }

    static Collection<TestCatalog> getCanNotCreateDatabase(Collection<TestCatalog> source) {
        return source.findAll { !it.createDatabase }
    }

    static Collection<TestCatalog> getCanCreateTable(Collection<TestCatalog> source) {
        return source.findAll { it.createTable }
    }

    static Collection<TestCatalog> getCanNotCreateTable(Collection<TestCatalog> source) {
        return source.findAll { !it.createTable }
    }

    static Collection<TestCatalog> getCanCreatePartition(Collection<TestCatalog> source) {
        return source.findAll { it.createPartition }
    }

    static Collection<TestCatalog> getCanDeleteDatabase(Collection<TestCatalog> source) {
        return source.findAll { it.deleteDatabase }
    }

    static Collection<TestCatalog> getCanNotDeleteDatabase(Collection<TestCatalog> source) {
        return source.findAll { !it.deleteDatabase }
    }

    static Collection<TestCatalog> getCanDeleteTable(Collection<TestCatalog> source) {
        return source.findAll { it.deleteTable }
    }

    static Collection<TestCatalog> getCanCreateView(Collection<TestCatalog> source) {
        return source.findAll { it.createView }
    }

    static Collection<TestCatalog> getCanNotDeleteTable(Collection<TestCatalog> source) {
        return source.findAll { !it.deleteTable }
    }

    static void resetAll() {
        TestCatalogs.ALL.each { catalog ->
            catalog.createdDatabases.clear()
            catalog.createdTables.clear()
            catalog.createdPartitions.clear()
        }
    }
}
