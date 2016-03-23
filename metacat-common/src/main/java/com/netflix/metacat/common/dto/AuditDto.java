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

package com.netflix.metacat.common.dto;

import com.wordnik.swagger.annotations.ApiModelProperty;

import java.util.Date;
import java.util.Objects;

@SuppressWarnings("unused")
public class AuditDto extends BaseDto {
    private static final long serialVersionUID = 9221109874202093789L;

    /* Created By */
    @ApiModelProperty(value = "User name who created the table", required = false)
    private String createdBy;
    /* Created date */
    @ApiModelProperty(value = "Creation date", required = false)
    private Date createdDate;
    /* Last modified by */
    @ApiModelProperty(value = "User name who last modified the table", required = false)
    private String lastModifiedBy;
    /* Last modified date */
    @ApiModelProperty(value = "Last modified date", required = false)
    private Date lastModifiedDate;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AuditDto)) return false;
        AuditDto auditDto = (AuditDto) o;
        return Objects.equals(createdBy, auditDto.createdBy) &&
                Objects.equals(createdDate, auditDto.createdDate) &&
                Objects.equals(lastModifiedBy, auditDto.lastModifiedBy) &&
                Objects.equals(lastModifiedDate, auditDto.lastModifiedDate);
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public Date getCreatedDate() {
        return createdDate;
    }

    public void setCreatedDate(Date createdDate) {
        this.createdDate = createdDate;
    }

    public String getLastModifiedBy() {
        return lastModifiedBy;
    }

    public void setLastModifiedBy(String lastModifiedBy) {
        this.lastModifiedBy = lastModifiedBy;
    }

    public Date getLastModifiedDate() {
        return lastModifiedDate;
    }

    public void setLastModifiedDate(Date lastModifiedDate) {
        this.lastModifiedDate = lastModifiedDate;
    }

    @Override
    public int hashCode() {
        return Objects.hash(createdBy, createdDate, lastModifiedBy, lastModifiedDate);
    }

}
