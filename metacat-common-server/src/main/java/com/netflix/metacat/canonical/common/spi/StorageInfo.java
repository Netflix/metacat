/*
 *  Copyright 2016 Netflix, Inc.
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *         http://www.apache.org/licenses/LICENSE-2.0
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */

package com.netflix.metacat.canonical.common.spi;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Storage info class.
 * @author zhenl
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class StorageInfo {
    /* Location of the data */
    private String uri;
    /* Input format of the file */
    private String inputFormat;
    /* Output format of the file */
    private String outputFormat;
    /* Serialization library */
    private String serializationLib;
    /* Serialization parameters */
    private Map<String, String> parameters;
    private Map<String, String> serdeInfoParameters;
}
