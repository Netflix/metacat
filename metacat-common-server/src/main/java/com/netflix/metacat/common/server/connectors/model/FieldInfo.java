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

package com.netflix.metacat.common.server.connectors.model;

import com.netflix.metacat.common.type.Type;
import com.wordnik.swagger.annotations.ApiModel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * Field DTO.
 * @since 1.0.0
 */
@ApiModel(value = "Table field/column metadata")
@SuppressWarnings("unused")
@Data
@EqualsAndHashCode(callSuper = false)
@NoArgsConstructor
@Builder
@AllArgsConstructor
public final class FieldInfo {
    private String comment;
    private String name;
    private boolean partitionKey;
    private String sourceType;
    private Type type;
    private Boolean isNullable;
    private Integer size;
    private String defaultValue;
    private Boolean isSortKey;
    private Boolean isIndexKey;
}
