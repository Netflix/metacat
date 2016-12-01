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
 * Info.
 */
@Entity
@javax.persistence.Table(name = "info",
    uniqueConstraints = @UniqueConstraint(name = "info_u1", columnNames = "location_id"))
public class Info extends IdEntity {
    private String inputFormat;
    private String outputFormat;
    private String serializationLib;
    private String owner;
    private Map<String, String> parameters;
    private Location location;

    @Column(name = "input_format")
    public String getInputFormat() {
        return inputFormat;
    }

    public void setInputFormat(final String inputFormat) {
        this.inputFormat = inputFormat;
    }

    @Column(name = "output_format")
    public String getOutputFormat() {
        return outputFormat;
    }

    public void setOutputFormat(final String outputFormat) {
        this.outputFormat = outputFormat;
    }

    @Column(name = "serialization_lib")
    public String getSerializationLib() {
        return serializationLib;
    }

    public void setSerializationLib(final String serializationLib) {
        this.serializationLib = serializationLib;
    }

    @Column(name = "owner")
    public String getOwner() {
        return owner;
    }

    public void setOwner(final String owner) {
        this.owner = owner;
    }

    @ElementCollection
    @MapKeyColumn(name = "parameters_idx")
    @Column(name = "parameters_elt")
    @CollectionTable(name = "info_parameters")
    public Map<String, String> getParameters() {
        return parameters;
    }

    public void setParameters(final Map<String, String> parameters) {
        this.parameters = parameters;
    }

    @OneToOne
    @JoinColumn(name = "location_id", nullable = false)
    public Location getLocation() {
        return location;
    }

    public void setLocation(final Location location) {
        this.location = location;
    }
}
