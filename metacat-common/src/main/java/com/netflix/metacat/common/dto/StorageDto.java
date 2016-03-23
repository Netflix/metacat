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

import java.util.Map;
import java.util.Objects;

/**
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StorageDto)) return false;
        StorageDto that = (StorageDto) o;
        return Objects.equals(inputFormat, that.inputFormat) &&
                Objects.equals(outputFormat, that.outputFormat) &&
                Objects.equals(owner, that.owner) &&
                Objects.equals(parameters, that.parameters) &&
                Objects.equals(serdeInfoParameters, that.serdeInfoParameters) &&
                Objects.equals(serializationLib, that.serializationLib) &&
                Objects.equals(uri, that.uri);
    }

    public String getInputFormat() {
        return inputFormat;
    }

    public void setInputFormat(String inputFormat) {
        this.inputFormat = inputFormat;
    }

    public String getOutputFormat() {
        return outputFormat;
    }

    public void setOutputFormat(String outputFormat) {
        this.outputFormat = outputFormat;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, String> parameters) {
        this.parameters = parameters;
    }

    public Map<String, String> getSerdeInfoParameters() {
        return serdeInfoParameters;
    }

    public void setSerdeInfoParameters(Map<String, String> serdeInfoParameters) {
        this.serdeInfoParameters = serdeInfoParameters;
    }

    public String getSerializationLib() {
        return serializationLib;
    }

    public void setSerializationLib(String serializationLib) {
        this.serializationLib = serializationLib;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    @Override
    public int hashCode() {
        return Objects.hash(inputFormat, outputFormat, owner, parameters, serdeInfoParameters, serializationLib, uri);
    }
}
