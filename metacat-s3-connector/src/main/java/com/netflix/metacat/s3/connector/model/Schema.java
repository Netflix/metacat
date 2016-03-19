package com.netflix.metacat.s3.connector.model;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.JoinColumn;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.UniqueConstraint;
import java.util.List;

/**
 * Created by amajumdar on 12/22/14.
 */
@Entity
@javax.persistence.Table(name="schema_object",
        uniqueConstraints= @UniqueConstraint(name="schema_object_u1",columnNames = "location_id"))
public class Schema extends IdEntity{
    private Location location;
    private List<Field> fields;

    @OneToOne
    @JoinColumn(name="location_id", nullable=false)
    public Location getLocation() {
        return location;
    }

    public void setLocation(Location location) {
        this.location = location;
    }

    @OneToMany(cascade= CascadeType.ALL, fetch= FetchType.LAZY, mappedBy = "schema")
    public List<Field> getFields() {
        return fields;
    }

    public void setFields(List<Field> fields) {
        this.fields = fields;
    }
}