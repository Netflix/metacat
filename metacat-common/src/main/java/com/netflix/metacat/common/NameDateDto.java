/*
 *
 *  Copyright 2016 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.metacat.common;

import com.netflix.metacat.common.dto.BaseDto;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Date;

/**
 * DTO containing the qualified name and the audit info.
 *
 * @author amajumdar
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class NameDateDto extends BaseDto {
    private static final long serialVersionUID = -5713826608609231492L;
    @Schema(description = "The date the entity was created")
    private Date createDate;
    @Schema(description = "The date the entity was last updated")
    private Date lastUpdated;
    @Schema(description = "The entity's name", requiredMode = Schema.RequiredMode.REQUIRED)
    private QualifiedName name;
}
