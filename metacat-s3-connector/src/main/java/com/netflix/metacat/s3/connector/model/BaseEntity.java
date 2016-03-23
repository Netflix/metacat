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

import org.joda.time.Instant;

import javax.persistence.Column;
import javax.persistence.MappedSuperclass;
import javax.persistence.PrePersist;
import javax.persistence.PreUpdate;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;

/**
 * {@code BaseEntity} is the entity that all entities.
 */
@MappedSuperclass
public class BaseEntity {

    /** The date of creation */
    protected Date createdDate;

    /** The last updated date. */
    protected Date lastUpdatedDate;

    /**
     * Get the date and time of the entity creation.
     *
     * @return
     * The date and time of the creation
     */
    @Column(name="date_created", insertable = true, updatable = false, nullable = false)
    public Date getCreatedDate() {
        return createdDate;
    }

    /**
     * Set the date and time of the creation.
     *
     * @param createdDate
     * The date and time of the creation
     */
    public void setCreatedDate(Date createdDate) {
        this.createdDate = createdDate;
    }

    public void setCreatedDate(Timestamp createdDate) {
        this.createdDate = createdDate;
    }

    /**
     * Get the date and time of the last update.
     *
     * @return
     * Get the date and time of the last update.
     */
    @Column(name = "last_updated", insertable = true, updatable = true, nullable = false)
    public Date getLastUpdatedDate() {
        return lastUpdatedDate;
    }

    /**
     * Set the date and time of the last update.
     *
     * @param lastUpdatedDate
     * The date and time of the last update
     */
    public void setLastUpdatedDate(Date lastUpdatedDate) {
        this.lastUpdatedDate = lastUpdatedDate;
    }

    public void setLastUpdatedDate(Timestamp lastUpdatedDate) {
        this.lastUpdatedDate = lastUpdatedDate;
    }

    @PrePersist
    public void onInsert() {
        setCreatedDate(Calendar.getInstance().getTime());
        setLastUpdatedDate(Instant.now().toDate());
    }

    @PreUpdate
    void onUpdate() {
        setLastUpdatedDate(Instant.now().toDate());
    }
    /**
     * Validate the entity for valid values.
     */
    public void validate() {}
}
