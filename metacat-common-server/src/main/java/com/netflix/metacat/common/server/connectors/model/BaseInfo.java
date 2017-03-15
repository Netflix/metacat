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

import com.netflix.metacat.common.QualifiedName;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Base class for catalog resources.
 * @author amajumdar
 * @since 1.0.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public abstract class BaseInfo {
    /* Name of the resource */
    private QualifiedName name;
    /* Audit information of the resource */
    private AuditInfo audit;
    /* Metadata properties of the resource */
    private Map<String, String> metadata;
}
