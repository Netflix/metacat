package com.netflix.metacat.s3.connector.model;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.JoinColumn;
import javax.persistence.OneToOne;
import javax.persistence.UniqueConstraint;

/**
 * Created by amajumdar on 12/22/14.
 */
@Entity
@javax.persistence.Table(name="location",
        uniqueConstraints= @UniqueConstraint(name="location_u1",columnNames = "table_id"))
public class Location extends IdEntity{
    /*
        static belongsTo = [table: Table]
    static hasOne = [schema: Schema, info: Info]
    //TODO: Serde info
    String uri
     */
    private String uri;
    private Table table;
    private Schema schema;
    private Info info;

    @Column(name = "uri", nullable = true)
    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    @OneToOne
    @JoinColumn(name="table_id", nullable=false)
    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    @OneToOne(cascade= CascadeType.ALL, fetch= FetchType.EAGER, mappedBy = "location")
    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    @OneToOne(cascade=CascadeType.ALL, fetch=FetchType.EAGER, mappedBy = "location")
    public Info getInfo() {
        return info;
    }

    public void setInfo(Info info) {
        this.info = info;
    }
}
