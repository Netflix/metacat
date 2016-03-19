package com.netflix.metacat.s3.connector.model;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.OneToMany;
import javax.persistence.UniqueConstraint;
import java.util.List;

/**
 * Created by amajumdar on 12/19/14.
 */
@Entity
@javax.persistence.Table(name="source",
        uniqueConstraints= @UniqueConstraint(name="source_u1",columnNames = "name"))
public class Source extends IdEntity{
    private String name;
    private String type;
    private String thriftUri;
    private boolean disabled = false;
    private List<Database> databases;

    @Column(name = "name", nullable = false)
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Column(name = "type", nullable = false)
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Column(name = "thrift_uri")
    public String getThriftUri() {
        return thriftUri;
    }

    public void setThriftUri(String thriftUri) {
        this.thriftUri = thriftUri;
    }

    @Column(name = "disabled", nullable = false)
    public boolean isDisabled() {
        return disabled;
    }

    public void setDisabled(boolean disabled) {
        this.disabled = disabled;
    }

    @OneToMany(cascade= CascadeType.ALL, fetch= FetchType.LAZY, mappedBy = "source")
    public List<Database> getDatabases() {
        return databases;
    }

    public void setDatabases(List<Database> databases) {
        this.databases = databases;
    }
}
