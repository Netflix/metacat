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

import lombok.Data;

import javax.annotation.Nullable;
import java.util.Date;
import java.util.UUID;

/**
 * The context of the request to Metacat.
 *
 * @author amajumdar
 * @author tgianos
 */
@Data
public class MetacatRequestContext {
    /**
     * Request header representing the user name.
     */
    public static final String HEADER_KEY_USER_NAME = "X-Netflix.user.name";
    /**
     * Request header representing the client application name.
     */
    public static final String HEADER_KEY_CLIENT_APP_NAME = "X-Netflix.client.app.name";
    /**
     * Request header representing the job id.
     */
    public static final String HEADER_KEY_JOB_ID = "X-Netflix.job.id";
    /**
     * Request header representing the data type context.
     */
    public static final String HEADER_KEY_DATA_TYPE_CONTEXT = "X-Netflix.data.type.context";

    private final String id = UUID.randomUUID().toString();
    // TODO: Move to Java 8 and use java.time.Instant
    private final long timestamp = new Date().getTime();
    private final String userName;
    private final String clientAppName;
    private final String clientId;
    private final String jobId;
    private final String dataTypeContext;

    /**
     * Constructor.
     *
     * @param userName        user name
     * @param clientAppName   client application name
     * @param clientId        client id
     * @param jobId           job id
     * @param dataTypeContext data type context
     */
    public MetacatRequestContext(
        @Nullable final String userName,
        @Nullable final String clientAppName,
        @Nullable final String clientId,
        @Nullable final String jobId,
        @Nullable final String dataTypeContext
    ) {
        this.userName = userName;
        this.clientAppName = clientAppName;
        this.clientId = clientId;
        this.jobId = jobId;
        this.dataTypeContext = dataTypeContext;
    }
}
