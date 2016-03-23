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

@ApiModel(value = "Table field/column metadata")
@SuppressWarnings("unused")
public class FieldDto extends BaseDto {
    private static final long serialVersionUID = 9096928516299407324L;

    @ApiModelProperty(value = "Comment of the field/column", required = false)
    private String comment;
    @ApiModelProperty(value = "Name of the field/column", required = true)
    private String name;
    @ApiModelProperty(value = "Is it a partition Key. If true, it is a partition key.", required = false)
    private boolean partition_key;
    @ApiModelProperty(value = "Position of the field/column", required = true)
    private Integer pos;
    @ApiModelProperty(value = "Source type of the field/column", required = false)
    private String source_type;
    @ApiModelProperty(value = "Type of the field/column", required = true)
    private String type;
    @ApiModelProperty(value = "Can the field/column be null", required = false)
    private Boolean isNullable;
    @ApiModelProperty(value = "Size of the field/column", required = false)
    private Integer size;
    @ApiModelProperty(value = "Default value of the column", required = false)
    private String defaultValue;
    @ApiModelProperty(value = "Is the column a sorted key", required = false)
    private Boolean isSortKey;
    @ApiModelProperty(value = "Is the column an index key", required = false)
    private Boolean isIndexKey;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FieldDto)) return false;
        FieldDto fieldDto = (FieldDto) o;
        return Objects.equals(partition_key, fieldDto.partition_key) &&
                Objects.equals(pos, fieldDto.pos) &&
                Objects.equals(comment, fieldDto.comment) &&
                Objects.equals(name, fieldDto.name) &&
                Objects.equals(source_type, fieldDto.source_type) &&
                Objects.equals(type, fieldDto.type) &&
                Objects.equals(isNullable, fieldDto.isNullable) &&
                Objects.equals(size, fieldDto.size) &&
                Objects.equals(defaultValue, fieldDto.defaultValue) &&
                Objects.equals(isSortKey, fieldDto.isSortKey) &&
                Objects.equals(isIndexKey, fieldDto.isIndexKey);
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getPos() {
        return pos;
    }

    public void setPos(Integer pos) {
        this.pos = pos;
    }

    public String getSource_type() {
        return source_type;
    }

    public void setSource_type(String source_type) {
        this.source_type = source_type;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(comment, name, partition_key, pos, source_type, type, isNullable, size, defaultValue, isSortKey, isIndexKey);
    }

    public boolean isPartition_key() {
        return partition_key;
    }

    public void setPartition_key(boolean partition_key) {
        this.partition_key = partition_key;
    }

    public Boolean getIsNullable() {
        return isNullable;
    }

    public void setIsNullable(Boolean isNullable) {
        this.isNullable = isNullable;
    }

    public Integer getSize() {
        return size;
    }

    public void setSize(Integer size) {
        this.size = size;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    public Boolean getIsSortKey() {
        return isSortKey;
    }

    public void setIsSortKey(Boolean isSortKey) {
        this.isSortKey = isSortKey;
    }

    public Boolean getIsIndexKey() {
        return isIndexKey;
    }

    public void setIsIndexKey(Boolean isIndexKey) {
        this.isIndexKey = isIndexKey;
    }
}
