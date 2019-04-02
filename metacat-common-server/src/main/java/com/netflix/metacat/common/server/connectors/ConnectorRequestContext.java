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
package com.netflix.metacat.common.server.connectors;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * The context of the request to Metacat.
 *
 * @author amajumdar
 * @author tgianos
 * @since 1.0.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ConnectorRequestContext {
    private long timestamp;
    private String userName;
    private boolean includeMetadata;
    //TODO: Move this to a response object.
    private boolean ignoreErrorsAfterUpdate;
}
