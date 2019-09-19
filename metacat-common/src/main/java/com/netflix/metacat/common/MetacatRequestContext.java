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

import lombok.Getter;

import javax.annotation.Nullable;
import java.util.Date;
import java.util.UUID;

/**
 * The context of the request to metacat.
 *
 * @author amajumdar
 * @author tgianos
 * @author zhenl
 */
@Getter
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

    /**
     * Default if unknown.
     */
    public static final String UNKNOWN = "UNKNOWN";

    private final String id = UUID.randomUUID().toString();
    // TODO: Move to Java 8 and use java.time.Instant
    private final long timestamp = new Date().getTime();

    private String userName;
    private final String clientAppName;
    private final String clientId;
    private final String jobId;
    private final String dataTypeContext;
    private final String apiUri;
    private final String scheme;

    /**
     * Constructor.
     */
    public MetacatRequestContext() {
        this.userName = null;
        this.clientAppName = null;
        this.clientId = null;
        this.jobId = null;
        this.dataTypeContext = null;
        this.apiUri = UNKNOWN;
        this.scheme = UNKNOWN;
    }

    /**
     * Constructor.
     *
     * @param userName        user name
     * @param clientAppName   client application name
     * @param clientId        client id
     * @param jobId           job id
     * @param dataTypeContext data type context
     * @param apiUri          the uri of rest api
     * @param scheme          http, thrift, internal, etc.
     */
    protected MetacatRequestContext(
        @Nullable final String userName,
        @Nullable final String clientAppName,
        @Nullable final String clientId,
        @Nullable final String jobId,
        @Nullable final String dataTypeContext,
        final String apiUri,
        final String scheme
    ) {
        this.userName = userName;
        this.clientAppName = clientAppName;
        this.clientId = clientId;
        this.jobId = jobId;
        this.dataTypeContext = dataTypeContext;
        this.apiUri = apiUri;
        this.scheme = scheme;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MetacatRequestContext{");
        sb.append("id='").append(id).append('\'');
        sb.append(", timestamp=").append(timestamp);
        sb.append(", userName='").append(userName).append('\'');
        sb.append(", clientAppName='").append(clientAppName).append('\'');
        sb.append(", clientId='").append(clientId).append('\'');
        sb.append(", jobId='").append(jobId).append('\'');
        sb.append(", dataTypeContext='").append(dataTypeContext).append('\'');
        sb.append(", apiUri='").append(apiUri).append('\'');
        sb.append(", scheme='").append(scheme).append('\'');
        sb.append('}');
        return sb.toString();
    }

    /**
     * Returns the username.
     * @param userName user name
     */
    public void setUserName(final String userName) {
        this.userName = userName;
    }

    /**
     * builder class for MetacatRequestContext.
     * @return the builder class for MetacatRequestContext
     */
    public static MetacatRequestContext.MetacatRequestContextBuilder builder() {
        return new MetacatRequestContext.MetacatRequestContextBuilder();
    }

    /**
     * MetacatRequestContext builder class.
     */
    public static class MetacatRequestContextBuilder {
        private String bUserName;
        private String bClientAppName;
        private String bClientId;
        private String bJobId;
        private String bDataTypeContext;
        private String bApiUri;
        private String bScheme;

        MetacatRequestContextBuilder() {
            this.bApiUri = UNKNOWN;
            this.bScheme = UNKNOWN;
        }

        /**
         * set userName.
         *
         * @param userName user name at client side
         * @return the builder
         */
        public MetacatRequestContext.MetacatRequestContextBuilder userName(@Nullable final String userName) {
            this.bUserName = userName;
            return this;
        }

        /**
         * set clientAppName.
         *
         * @param clientAppName application name of client
         * @return the builder
         */
        public MetacatRequestContext.MetacatRequestContextBuilder clientAppName(@Nullable final String clientAppName) {
            this.bClientAppName = clientAppName;
            return this;
        }

        /**
         * set clientId.
         *
         * @param clientId client identifier, such as host name
         * @return the builder
         */
        public MetacatRequestContext.MetacatRequestContextBuilder clientId(@Nullable final String clientId) {
            this.bClientId = clientId;
            return this;
        }

        /**
         * set jobId.
         *
         * @param jobId jobid from client side
         * @return the builder
         */
        public MetacatRequestContext.MetacatRequestContextBuilder jobId(@Nullable final String jobId) {
            this.bJobId = jobId;
            return this;
        }

        /**
         * set datatypeContext.
         *
         * @param dataTypeContext the data type context in rest api
         * @return the builder
         */
        public MetacatRequestContext.MetacatRequestContextBuilder dataTypeContext(
            @Nullable final String dataTypeContext) {
            this.bDataTypeContext = dataTypeContext;
            return this;
        }

        /**
         * set apiUri.
         *
         * @param apiUri the uri in rest api
         * @return the builder
         */
        public MetacatRequestContext.MetacatRequestContextBuilder apiUri(final String apiUri) {
            this.bApiUri = apiUri;
            return this;
        }

        /**
         * set scheme.
         *
         * @param scheme the scheme component in restapi such as http
         * @return the builder
         */
        public MetacatRequestContext.MetacatRequestContextBuilder scheme(final String scheme) {
            this.bScheme = scheme;
            return this;
        }

        /**
         * builder.
         *
         * @return MetacatRequestContext object
         */
        public MetacatRequestContext build() {
            return new MetacatRequestContext(this.bUserName,
                this.bClientAppName,
                this.bClientId,
                this.bJobId,
                this.bDataTypeContext,
                this.bApiUri,
                this.bScheme);
        }
    }
}
