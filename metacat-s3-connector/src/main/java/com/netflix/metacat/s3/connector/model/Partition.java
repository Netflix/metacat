package com.netflix.metacat.s3.connector.model;

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
 * Created by amajumdar on 12/22/14.
 */
@Entity
@javax.persistence.Table(name="partition_table",
        indexes = { @Index(name="partition_table_i1", columnList = "name"),@Index(name="partition_table_i2",columnList = "uri") },
        uniqueConstraints= @UniqueConstraint(name="partition_table_u1",columnNames = {"table_id", "name"}))
@NamedQueries({
        @NamedQuery(
                name = Partition.NAME_QUERY_GET_FOR_TABLE,
                query = "select p from Partition p where p.table.name=:tableName and p.table.database.name=:databaseName and p.table.database.source.name=:sourceName"
        ),
        @NamedQuery(
                name = Partition.NAME_QUERY_GET_COUNT_FOR_TABLE,
                query = "select count(p) from Partition p where p.table.name=:tableName and p.table.database.name=:databaseName and p.table.database.source.name=:sourceName"
        ),
        @NamedQuery(
                name = Partition.NAME_QUERY_DELETE_BY_PARTITION_NAMES,
                query = "delete from Partition p where p.table.id = (select t.id from Table t where t.name=:tableName and t.database.name=:databaseName and t.database.source.name=:sourceName) and p.name in (:partitionNames)"
        )
        ,
        @NamedQuery(
                name = Partition.NAME_QUERY_GET_BY_URI,
                query = "select p from Partition p where p.uri=:uri"
        ),
        @NamedQuery(
                name = Partition.NAME_QUERY_GET_BY_URI_PREFIX,
                query = "select p from Partition p where p.uri like :uri"
        )
})
public class Partition extends IdEntity{
    public static final String NAME_QUERY_GET_FOR_TABLE = "getForTable";
    public static final String NAME_QUERY_GET_COUNT_FOR_TABLE = "getCountForTable";
    public static final String NAME_QUERY_DELETE_BY_PARTITION_NAMES = "deleteByPartitionNames";
    public static final String NAME_QUERY_GET_BY_URI = "getByUri";
    public static final String NAME_QUERY_GET_BY_URI_PREFIX = "getByUriPrefix";
    private String name;
    private String uri;
    private Table table;

    @Column(name = "name", nullable = false)
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Column(name = "uri", nullable = false)
    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    @ManyToOne(fetch = FetchType.LAZY, optional=false )
    @JoinColumn(name = "table_id", nullable = false)
    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }
}
