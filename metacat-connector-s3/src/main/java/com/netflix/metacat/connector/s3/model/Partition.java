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

package com.netflix.metacat.connector.s3.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Index;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.UniqueConstraint;

/**
 * Partition.
 */
@Entity
@javax.persistence.Table(name = "partition_table",
    indexes = { @Index(name = "partition_table_i1", columnList = "name"),
        @Index(name = "partition_table_i2", columnList = "uri") },
    uniqueConstraints = @UniqueConstraint(name = "partition_table_u1", columnNames = { "table_id", "name" }))
@NamedQueries({
    @NamedQuery(
        name = Partition.NAME_QUERY_GET_FOR_TABLE,
        query = "select p from Partition p where p.table.name=:tableName and p.table.database.name=:databaseName"
            + " and p.table.database.source.name=:sourceName"
    ),
    @NamedQuery(
        name = Partition.NAME_QUERY_GET_COUNT_FOR_TABLE,
        query = "select count(p) from Partition p where p.table.name=:tableName"
            + " and p.table.database.name=:databaseName and p.table.database.source.name=:sourceName"
    ),
    @NamedQuery(
        name = Partition.NAME_QUERY_DELETE_BY_PARTITION_NAMES,
        query = "delete from Partition p where p.table.id = (select t.id from Table t where t.name=:tableName"
            + " and t.database.name=:databaseName and t.database.source.name=:sourceName)"
            + " and p.name in (:partitionNames)"
    ),
    @NamedQuery(
        name = Partition.NAME_QUERY_GET_BY_URI,
        query = "select p from Partition p where p.uri in :uris"
    ),
    @NamedQuery(
        name = Partition.NAME_QUERY_GET_BY_URI_PREFIX,
        query = "select p from Partition p where p.uri like :uri"
    )
})
public class Partition extends IdEntity {
    /** Query name to get partition for a given table. */
    public static final String NAME_QUERY_GET_FOR_TABLE = "getForTable";
    /** Query name to get partition count for a given table. */
    public static final String NAME_QUERY_GET_COUNT_FOR_TABLE = "getCountForTable";
    /** Query name to delete. */
    public static final String NAME_QUERY_DELETE_BY_PARTITION_NAMES = "deleteByPartitionNames";
    /** Query name to get partition for a given uri . */
    public static final String NAME_QUERY_GET_BY_URI = "getByUri";
    /** Query name to get partition for a given uri prefix. */
    public static final String NAME_QUERY_GET_BY_URI_PREFIX = "getByUriPrefix";
    private String name;
    private String uri;
    private Table table;

    @Column(name = "name", nullable = false)
    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    @Column(name = "uri", nullable = false)
    public String getUri() {
        return uri;
    }

    public void setUri(final String uri) {
        this.uri = uri;
    }

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "table_id", nullable = false)
    public Table getTable() {
        return table;
    }

    public void setTable(final Table table) {
        this.table = table;
    }
}
