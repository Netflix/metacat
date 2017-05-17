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
package com.netflix.metacat.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationModule;
import com.google.common.base.Preconditions;
import com.netflix.metacat.client.module.JacksonDecoder;
import com.netflix.metacat.client.module.JacksonEncoder;
import com.netflix.metacat.client.module.MetacatErrorDecoder;
import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.api.MetacatV1;
import com.netflix.metacat.common.api.MetadataV1;
import com.netflix.metacat.common.api.PartitionV1;
import com.netflix.metacat.common.api.ResolverV1;
import com.netflix.metacat.common.json.MetacatJsonLocator;
import feign.Feign;
import feign.Request;
import feign.RequestInterceptor;
import feign.RequestTemplate;
import feign.Retryer;
import feign.jaxrs.JAXRSContract;
import feign.slf4j.Slf4jLogger;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import java.util.concurrent.TimeUnit;

/**
 * Client to communicate with Metacat.  This version depends on the Feign library.
 *
 * @author amajumdar
 */
@Slf4j
public final class Client {
    private final MetacatV1 api;
    private final Feign.Builder feignBuilder;
    private final String host;
    private final PartitionV1 partitionApi;
    private final MetadataV1 metadataApi;
    private final ResolverV1 resolverApi;

    private Client(
        @Nonnull @NonNull final String host,
        @Nonnull @NonNull final feign.Client client,
        @Nonnull @NonNull final feign.Logger.Level logLevel,
        @Nonnull @NonNull final RequestInterceptor requestInterceptor,
        @Nonnull @NonNull final Retryer retryer,
        @Nonnull @NonNull final Request.Options options
    ) {
        final ObjectMapper mapper = MetacatJsonLocator.INSTANCE
            .getPrettyObjectMapper()
            .copy()
            .registerModule(new GuavaModule())
            .registerModule(new JaxbAnnotationModule());

        log.info("Connecting to {}", host);
        this.host = host;

        feignBuilder = Feign.builder()
            .client(client)
            .logger(new Slf4jLogger())
            .logLevel(logLevel)
            .contract(new JAXRSContract())
            .encoder(new JacksonEncoder(mapper))
            .decoder(new JacksonDecoder(mapper))
            .errorDecoder(new MetacatErrorDecoder())
            .requestInterceptor(requestInterceptor)
            .retryer(retryer)
            .options(options);

        api = getApiClient(MetacatV1.class);
        partitionApi = getApiClient(PartitionV1.class);
        metadataApi = getApiClient(MetadataV1.class);
        resolverApi = getApiClient(ResolverV1.class);
    }

    /**
     * Returns the client builder.
     *
     * @return Builder to create the metacat client
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns an API instance that conforms to the given API Type that can communicate with the Metacat server.
     *
     * @param apiType apiType A JAX-RS annotated Metacat interface
     * @param <T>     API Resource instance
     * @return An instance that implements the given interface and is wired up to communicate with the Metacat server.
     */
    public <T> T getApiClient(
        @Nonnull @NonNull final Class<T> apiType
    ) {
        Preconditions.checkArgument(apiType.isInterface(), "apiType must be an interface");

        return feignBuilder.target(apiType, host);
    }

    /**
     * Return an API instance that can be used to interact with the metacat server.
     *
     * @return An instance api conforming to MetacatV1 interface
     */
    public MetacatV1 getApi() {
        return api;
    }

    /**
     * Return an API instance that can be used to interact with the metacat server for partitions.
     *
     * @return An instance api conforming to PartitionV1 interface
     */
    public PartitionV1 getPartitionApi() {
        return partitionApi;
    }

    /**
     * Return an API instance that can be used to interact with the metacat server for only user metadata.
     *
     * @return An instance api conforming to MetadataV1 interface
     */
    public MetadataV1 getMetadataApi() {
        return metadataApi;
    }

    /**
     * Return an API instance that can be used to interact with
     * the metacat server for getting the qualified name by uri.
     *
     * @return An instance api conforming to ResolverV1 interface
     */
    public ResolverV1 getResolverApi() {
        return resolverApi;
    }

    /**
     * Builder class to build the metacat client.
     */
    public static class Builder {
        private String host;
        private String userName;
        private feign.Client client;
        private String clientAppName;
        private String jobId;
        private String dataTypeContext;
        private feign.Logger.Level logLevel;
        private Retryer retryer;
        private RequestInterceptor requestInterceptor;
        private Request.Options requestOptions;

        /**
         * Sets the log level for the client.
         *
         * @param clientLogLevel log level
         * @return Builder
         */
        public Builder withLogLevel(final feign.Logger.Level clientLogLevel) {
            this.logLevel = clientLogLevel;
            return this;
        }

        /**
         * Sets the server host name.
         *
         * @param serverHost server host to connect
         * @return Builder
         */
        public Builder withHost(final String serverHost) {
            this.host = serverHost;
            return this;
        }

        /**
         * Sets the retryer logic for the client.
         *
         * @param clientRetryer retry implementation
         * @return Builder
         */
        public Builder withRetryer(final Retryer clientRetryer) {
            this.retryer = clientRetryer;
            return this;
        }

        /**
         * Sets the user name to pass in the request header.
         *
         * @param requestUserName user name
         * @return Builder
         */
        public Builder withUserName(final String requestUserName) {
            this.userName = requestUserName;
            return this;
        }

        /**
         * Sets the application name to pass in the request header.
         *
         * @param appName application name
         * @return Builder
         */
        public Builder withClientAppName(final String appName) {
            this.clientAppName = appName;
            return this;
        }

        /**
         * Sets the job id to pass in the request header.
         *
         * @param clientJobId job id
         * @return Builder
         */
        public Builder withJobId(final String clientJobId) {
            this.jobId = clientJobId;
            return this;
        }

        /**
         * Sets the Client implementation to use.
         *
         * @param feignClient Feign Client
         * @return Builder
         */
        public Builder withClient(final feign.Client feignClient) {
            this.client = feignClient;
            return this;
        }

        /**
         * Sets the data type context to pass in the request header.
         *
         * @param requestDataTypeContext Data type conext
         * @return Builder
         */
        public Builder withDataTypeContext(final String requestDataTypeContext) {
            this.dataTypeContext = requestDataTypeContext;
            return this;
        }

        /**
         * Sets the request interceptor.
         *
         * @param clientRrequestInterceptor request interceptor
         * @return Builder
         */
        public Builder withRequestInterceptor(final RequestInterceptor clientRrequestInterceptor) {
            this.requestInterceptor = clientRrequestInterceptor;
            return this;
        }

        /**
         * Sets the request options.
         *
         * @param clientRequestOptions request options
         * @return Builder
         */
        public Builder withRequestOptions(final Request.Options clientRequestOptions) {
            this.requestOptions = clientRequestOptions;
            return this;
        }

        /**
         * Builds the Metacat client.
         *
         * @return Client that can be used to make metacat API calls.
         */
        public Client build() {
            Preconditions.checkArgument(userName != null, "User name cannot be null");
            Preconditions.checkArgument(clientAppName != null, "Client application name cannot be null");
            if (host == null) {
                host = System.getProperty("netflix.metacat.host", System.getenv("NETFLIX_METACAT_HOST"));
            }
            Preconditions.checkArgument(host != null, "Host cannot be null");
            if (retryer == null) {
                retryer = new Retryer.Default(TimeUnit.MINUTES.toMillis(30), TimeUnit.MINUTES.toMillis(30), 0);
            }
            final RequestInterceptor interceptor = new RequestInterceptor() {
                @Override
                public void apply(final RequestTemplate template) {
                    if (requestInterceptor != null) {
                        requestInterceptor.apply(template);
                    }
                    template.header(MetacatRequestContext.HEADER_KEY_USER_NAME, userName);
                    template.header(MetacatRequestContext.HEADER_KEY_CLIENT_APP_NAME, clientAppName);
                    template.header(MetacatRequestContext.HEADER_KEY_JOB_ID, jobId);
                    template.header(MetacatRequestContext.HEADER_KEY_DATA_TYPE_CONTEXT, dataTypeContext);
                }
            };
            if (requestOptions == null) {
                requestOptions = new Request.Options((int) TimeUnit.MINUTES.toMillis(10),
                    (int) TimeUnit.MINUTES.toMillis(30));
            }
            if (logLevel == null) {
                logLevel = feign.Logger.Level.NONE;
            }
            if (client == null) {
                client = new feign.Client.Default(null, null);
            }
            return new Client(host, client, logLevel, interceptor, retryer, requestOptions);
        }
    }
}
