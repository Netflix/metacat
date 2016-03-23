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

package com.netflix.metacat.common;

import com.netflix.metacat.common.dto.BaseDto;
import com.wordnik.swagger.annotations.ApiModelProperty;

import java.util.Date;
import java.util.Objects;

public class NameDateDto extends BaseDto {
    private static final long serialVersionUID = -5713826608609231492L;
    @ApiModelProperty(value = "The date the entity was created", required = false)
    private Date createDate;
    @ApiModelProperty(value = "The date the entity was last updated", required = false)
    private Date lastUpdated;
    @ApiModelProperty(value = "The entity's name", required = true)
    private QualifiedName name;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NameDateDto)) return false;
        NameDateDto that = (NameDateDto) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(createDate, that.createDate) &&
                Objects.equals(lastUpdated, that.lastUpdated);
    }

    public Date getCreateDate() {
        return createDate;
    }

    public void setCreateDate(Date createDate) {
        this.createDate = createDate;
    }

    public Date getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(Date lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    public QualifiedName getName() {
        return name;
    }

    public void setName(QualifiedName name) {
        this.name = name;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, createDate, lastUpdated);
    }
}
