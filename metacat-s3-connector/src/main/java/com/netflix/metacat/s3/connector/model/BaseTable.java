package com.netflix.metacat.s3.connector.model;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.FetchType;
import javax.persistence.MappedSuperclass;
import javax.persistence.OneToMany;
import java.util.List;

/**
 * Created by amajumdar on 12/23/14.
 */
@MappedSuperclass
public abstract class BaseTable extends IdEntity{
    private String name;
    private List<Partition> partitions;

    @Column(name = "name", nullable = false)
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @OneToMany(cascade= CascadeType.ALL, fetch= FetchType.LAZY, mappedBy = "table")
    public List<Partition> getPartitions() {
        return partitions;
    }

    public void setPartitions(List<Partition> partitions) {
        this.partitions = partitions;
    }
}
