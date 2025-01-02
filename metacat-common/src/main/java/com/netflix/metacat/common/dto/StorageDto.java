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

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Storage DTO.
 * <pre>
 * {
 * "inputFormat": "org.apache.hadoop.mapred.TextInputFormat",
 * "outputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
 * "serializationLib": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
 * "parameters": {
 * "serialization.format": "1"
 * },
 * "owner": "charsmith"
 * }
 * </pre>
 */
@Schema(description = "Serialization/Deserialization metadata of the table data")
@SuppressWarnings("unused")
@Data
@EqualsAndHashCode(callSuper = false)
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StorageDto extends BaseDto {
    private static final long serialVersionUID = 4933906340321707232L;

    @Schema(description = "Input format of the table data stored")
    private String inputFormat;
    @Schema(description = "Output format of the table data stored")
    private String outputFormat;
    @Schema(description = "Owner of the table")
    private String owner;
    @Schema(description = "Extra storage parameters")
    private Map<String, String> parameters;
    @Schema(description = "Extra storage parameters")
    private Map<String, String> serdeInfoParameters;
    @Schema(description = "Serialization library of the data")
    private String serializationLib;
    @Schema(description = "URI of the table. Only applies to certain data sources like hive, S3")
    private String uri;
}
