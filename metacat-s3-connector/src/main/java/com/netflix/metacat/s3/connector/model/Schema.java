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