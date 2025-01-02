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
package com.netflix.metacat.common.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * The name and type of a catalog.
 */
@Schema(description = "The name and type of a catalog")
@SuppressWarnings("unused")
@Data
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class CatalogMappingDto extends BaseDto {
    private static final long serialVersionUID = -1223516438943164936L;

    @Schema(description = "The name of the catalog", requiredMode = Schema.RequiredMode.REQUIRED)
    private String catalogName;
    @Schema(description = "The connector type of the catalog", requiredMode = Schema.RequiredMode.REQUIRED)
    private String connectorName;
    @Schema(description = "Cluster information referred by this catalog", requiredMode = Schema.RequiredMode.REQUIRED)
    @JsonProperty
    private ClusterDto clusterDto;
}
