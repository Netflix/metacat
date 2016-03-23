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

import com.wordnik.swagger.annotations.ApiModel;
import com.wordnik.swagger.annotations.ApiModelProperty;

import java.util.Objects;

/**
 * Information required to create a new catalog
 */
@ApiModel("Information required to create a new catalog")
public class CreateCatalogDto extends BaseDto {
    private static final long serialVersionUID = -5037037662666608796L;

    @ApiModelProperty(value = "the type of the connector of this catalog", required = true)
    private String type;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CreateCatalogDto)) return false;
        CreateCatalogDto that = (CreateCatalogDto) o;
        return Objects.equals(type, that.type);
    }

    /**
     * @return the name of the connector
     */
    public String getType() {
        return type;
    }

    /**
     * @param type the name of the connector used by this catalog
     */
    public void setType(String type) {
        this.type = type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type);
    }

}
