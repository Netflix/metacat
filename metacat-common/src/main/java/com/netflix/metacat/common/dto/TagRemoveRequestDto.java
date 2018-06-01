/*
 *  Copyright 2018 Netflix, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.netflix.metacat.common.dto;

import com.netflix.metacat.common.QualifiedName;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

/**
 * Tag Remove Request Dto.
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class TagRemoveRequestDto extends BaseDto {
    private static final long serialVersionUID = 8698531483258796673L;
    @ApiModelProperty(value = "the name of this entity", required = true)
    private QualifiedName name;

    @ApiModelProperty(value = "flag to delete all tags")
    private Boolean deleteAll;

    @ApiModelProperty(value = "Tags to insert")
    private List<String> tags;
}
