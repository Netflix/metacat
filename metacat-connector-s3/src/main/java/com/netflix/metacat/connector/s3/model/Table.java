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

import jakarta.persistence.CascadeType;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Index;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.NamedQueries;
import jakarta.persistence.NamedQuery;
import jakarta.persistence.OneToOne;
import jakarta.persistence.UniqueConstraint;

/**
 * Table.
 */
@Entity
@jakarta.persistence.Table(name = "table_object",
    indexes = { @Index(name = "table_object_i1", columnList = "name") },
    uniqueConstraints = @UniqueConstraint(name = "table_object_u1", columnNames = { "database_id", "name" }))
@NamedQueries({
    @NamedQuery(
        name = Table.NAME_QUERY_GET_BY_SOURCE_DATABASE_TABLE_NAMES,
        query = "select t from Table t where t.database.source.name=:sourceName and t.database.name=:databaseName"
            + " and t.name in (:tableNames)"
    )
})
public class Table extends BaseTable {
    /** Query name to get table for the given source, database and table names. */
    public static final String NAME_QUERY_GET_BY_SOURCE_DATABASE_TABLE_NAMES = "getBySourceDatabaseTableNames";
    private Database database;
    private Location location;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "database_id", nullable = false)
    public Database getDatabase() {
        return database;
    }

    public void setDatabase(final Database database) {
        this.database = database;
    }

    @OneToOne(cascade = CascadeType.ALL, fetch = FetchType.EAGER, mappedBy = "table")
    public Location getLocation() {
        return location;
    }

    public void setLocation(final Location location) {
        this.location = location;
    }
}
