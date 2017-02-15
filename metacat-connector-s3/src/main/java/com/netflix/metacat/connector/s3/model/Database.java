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

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Index;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.OneToMany;
import javax.persistence.UniqueConstraint;
import java.util.List;

/**
 * Database.
 */
@Entity
@javax.persistence.Table(name = "database_object",
    indexes = @Index(name = "database_object_i1", columnList = "name"),
    uniqueConstraints = @UniqueConstraint(name = "database_object_u1", columnNames = { "source_id", "name" }))
@NamedQueries({
    @NamedQuery(
        name = Database.NAME_QUERY_GET_BY_SOURCE_DATABASE_NAMES,
        query = "select d from Database d where d.source.name=:sourceName and d.name in (:databaseNames)"
    )
})
public class Database extends IdEntity {
    /** Named query name. */
    public static final String NAME_QUERY_GET_BY_SOURCE_DATABASE_NAMES = "getBySourceDatabaseNames";
    private String name;
    private List<Table> tables;
    private Source source;

    @Column(name = "name", nullable = false)
    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    @OneToMany(cascade = CascadeType.ALL, fetch = FetchType.LAZY, mappedBy = "database")
    public List<Table> getTables() {
        return tables;
    }

    public void setTables(final List<Table> tables) {
        this.tables = tables;
    }

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "source_id", nullable = false)
    public Source getSource() {
        return source;
    }

    public void setSource(final Source source) {
        this.source = source;
    }
}
