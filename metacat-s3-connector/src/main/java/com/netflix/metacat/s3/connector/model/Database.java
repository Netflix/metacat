package com.netflix.metacat.s3.connector.model;

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
 * Created by amajumdar on 12/19/14.
 */
@Entity
@javax.persistence.Table(name="database_object",
        indexes =  @Index(name="database_object_i1", columnList = "name"),
        uniqueConstraints= @UniqueConstraint(name="database_object_u1", columnNames = {"source_id", "name"}))
@NamedQueries({
        @NamedQuery(
                name = Database.NAME_QUERY_GET_BY_SOURCE_DATABASE_NAMES,
                query = "select d from Database d where d.source.name=:sourceName and d.name in (:databaseNames)"
        )
})
public class Database extends IdEntity{
    public static final String NAME_QUERY_GET_BY_SOURCE_DATABASE_NAMES = "getBySourceDatabaseNames";
    private String name;
    private List<Table> tables;
    private Source source;

    @Column(name = "name", nullable = false)
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
    @OneToMany(cascade=CascadeType.ALL, fetch= FetchType.LAZY, mappedBy = "database")
    public List<Table> getTables() {
        return tables;
    }

    public void setTables(List<Table> tables) {
        this.tables = tables;
    }
    @ManyToOne(fetch = FetchType.LAZY, optional=false)
    @JoinColumn(name = "source_id", nullable = false)
    public Source getSource() {
        return source;
    }

    public void setSource(Source source) {
        this.source = source;
    }
}
