package com.netflix.metacat.s3.connector.model;

import javax.persistence.CollectionTable;
import javax.persistence.Column;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.JoinColumn;
import javax.persistence.MapKeyColumn;
import javax.persistence.OneToOne;
import javax.persistence.UniqueConstraint;
import java.util.Map;

/**
 * Created by amajumdar on 12/22/14.
 */
@Entity
@javax.persistence.Table(name="info",
        uniqueConstraints= @UniqueConstraint(name="info_u1",columnNames = "location_id"))
public class Info extends IdEntity{
    private String inputFormat;
    private String outputFormat;
    private String serializationLib;
    private String owner;
    private Map<String, String> parameters;
    private Location location;

    @Column(name="input_format")
    public String getInputFormat() {
        return inputFormat;
    }

    public void setInputFormat(String inputFormat) {
        this.inputFormat = inputFormat;
    }

    @Column(name="output_format")
    public String getOutputFormat() {
        return outputFormat;
    }

    public void setOutputFormat(String outputFormat) {
        this.outputFormat = outputFormat;
    }

    @Column(name="serialization_lib")
    public String getSerializationLib() {
        return serializationLib;
    }

    public void setSerializationLib(String serializationLib) {
        this.serializationLib = serializationLib;
    }

    @Column(name="owner")
    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    @ElementCollection
    @MapKeyColumn(name="parameters_idx")
    @Column(name="parameters_elt")
    @CollectionTable(name="info_parameters")
    public Map<String, String> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, String> parameters) {
        this.parameters = parameters;
    }

    @OneToOne
    @JoinColumn(name="location_id", nullable=false)
    public Location getLocation() {
        return location;
    }

    public void setLocation(Location location) {
        this.location = location;
    }
}
