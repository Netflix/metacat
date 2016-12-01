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
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Map;

/**
 * Storage DTO.
 * <pre>
 {
 "inputFormat": "org.apache.hadoop.mapred.TextInputFormat",
 "outputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
 "serializationLib": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
 "parameters": {
 "serialization.format": "1"
 },
 "owner": "charsmith"
 }
 * </pre>
 */
@ApiModel("Serialization/Deserialization metadata of the table data")
@SuppressWarnings("unused")
@Data
@EqualsAndHashCode(callSuper = false)
public class StorageDto extends BaseDto {
    private static final long serialVersionUID = 4933906340321707232L;

    @ApiModelProperty(value = "Input format of the table data stored", required = false)
    private String inputFormat;
    @ApiModelProperty(value = "Output format of the table data stored", required = false)
    private String outputFormat;
    @ApiModelProperty(value = "Owner of the table", required = false)
    private String owner;
    @ApiModelProperty(value = "Extra storage parameters", required = false)
    private Map<String, String> parameters;
    @ApiModelProperty(value = "Extra storage parameters", required = false)
    private Map<String, String> serdeInfoParameters;
    @ApiModelProperty(value = "Serialization library of the data", required = false)
    private String serializationLib;
    @ApiModelProperty(value = "URI of the table. Only applies to certain data sources like hive, S3", required = false)
    private String uri;
}
