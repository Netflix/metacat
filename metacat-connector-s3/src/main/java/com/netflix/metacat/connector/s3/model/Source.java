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
import javax.persistence.OneToMany;
import javax.persistence.UniqueConstraint;
import java.util.List;

/**
 * Source.
 */
@Entity
@javax.persistence.Table(name = "source",
    uniqueConstraints = @UniqueConstraint(name = "source_u1", columnNames = "name"))
public class Source extends IdEntity {
    private String name;
    private String type;
    private String thriftUri;
    private boolean disabled;
    private List<Database> databases;

    @Column(name = "name", nullable = false)
    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    @Column(name = "type", nullable = false)
    public String getType() {
        return type;
    }

    public void setType(final String type) {
        this.type = type;
    }

    @Column(name = "thrift_uri")
    public String getThriftUri() {
        return thriftUri;
    }

    public void setThriftUri(final String thriftUri) {
        this.thriftUri = thriftUri;
    }

    @Column(name = "disabled", nullable = false)
    public boolean isDisabled() {
        return disabled;
    }

    public void setDisabled(final boolean disabled) {
        this.disabled = disabled;
    }

    @OneToMany(cascade = CascadeType.ALL, fetch = FetchType.LAZY, mappedBy = "source")
    public List<Database> getDatabases() {
        return databases;
    }

    public void setDatabases(final List<Database> databases) {
        this.databases = databases;
    }
}
