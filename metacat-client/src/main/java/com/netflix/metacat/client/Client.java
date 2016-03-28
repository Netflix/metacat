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

package com.netflix.metacat.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationModule;
import com.netflix.metacat.client.module.JacksonDecoder;
import com.netflix.metacat.client.module.JacksonEncoder;
import com.netflix.metacat.client.module.MetacatErrorDecoder;
import com.netflix.metacat.common.MetacatContext;
import com.netflix.metacat.common.api.MetacatV1;
import com.netflix.metacat.common.api.MetadataV1;
import com.netflix.metacat.common.api.PartitionV1;
import com.netflix.metacat.common.json.MetacatJsonLocator;
import feign.Feign;
import feign.Request;
import feign.RequestInterceptor;
import feign.RequestTemplate;
import feign.Retryer;
import feign.jaxrs.JAXRSContract;
import feign.slf4j.Slf4jLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Client to communicate with Metacat.  This version depends on the Feign library.
 */
public class Client {
    private static final Logger log = LoggerFactory.getLogger(Client.class);
    private final MetacatV1 api;
    private final Feign.Builder feignBuilder;
    private final String host;
    private final PartitionV1 partitionApi;
    private final MetadataV1 metadataApi;

    private Client(
            @Nonnull String host,
            @Nonnull feign.Logger.Level logLevel,
            @Nonnull RequestInterceptor requestInterceptor,
            @Nonnull Retryer retryer,
            @Nonnull Request.Options options
    ) {
        ObjectMapper mapper = MetacatJsonLocator.INSTANCE
                .getPrettyObjectMapper()
                .copy()
                .registerModule(new GuavaModule())
                .registerModule(new JaxbAnnotationModule());

        log.info("Connecting to {}", host);
        this.host = host;

        feignBuilder = Feign.builder()
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
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String host;
        private String userName;
        private String clientAppName;
        private String jobId;
        private String dataTypeContext;
        private feign.Logger.Level logLevel;
        private Retryer retryer;
        private RequestInterceptor requestInterceptor;
        private Request.Options requestOptions;

        public Builder withLogLevel(feign.Logger.Level logLevel) {
            this.logLevel = logLevel;
            return this;
        }

        public Builder withHost(String host) {
            this.host = host;
            return this;
        }

        public Builder withRetryer(Retryer retryer) {
            this.retryer = retryer;
            return this;
        }

        public Builder withUserName(String userName) {
            this.userName = userName;
            return this;
        }

        public Builder withClientAppName(String appName) {
            this.clientAppName = appName;
            return this;
        }

        public Builder withJobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        public Builder withDataTypeContext(String dataTypeContext) {
            this.dataTypeContext = dataTypeContext;
            return this;
        }

        public Builder withRequestInterceptor(RequestInterceptor requestInterceptor) {
            this.requestInterceptor = requestInterceptor;
            return this;
        }


        public Builder withRequestOptions(Request.Options requestOptions) {
            this.requestOptions = requestOptions;
            return this;
        }

        public Client build() {
            checkArgument(userName != null, "User name cannot be null");
            checkArgument(clientAppName != null, "Client application name cannot be null");
            if(host == null){
                host = System.getProperty("netflix.metacat.host", System.getenv("NETFLIX_METACAT_HOST"));
            }
            checkArgument(host != null, "Host cannot be null");
            if( retryer == null){
                retryer = new Retryer.Default(TimeUnit.MINUTES.toMillis(30), TimeUnit.MINUTES.toMillis(30), 0);
            }
            RequestInterceptor interceptor = new RequestInterceptor() {
                @Override
                public void apply(RequestTemplate template) {
                    if( requestInterceptor != null) {
                        requestInterceptor.apply(template);
                    }
                    template.header(MetacatContext.HEADER_KEY_USER_NAME, userName);
                    template.header(MetacatContext.HEADER_KEY_CLIENT_APP_NAME, clientAppName);
                    template.header(MetacatContext.HEADER_KEY_JOB_ID, jobId);
                    template.header(MetacatContext.HEADER_KEY_DATA_TYPE_CONTEXT, dataTypeContext);
                }
            };
            if( requestOptions == null){
                requestOptions = new Request.Options((int)TimeUnit.MINUTES.toMillis(10), (int)TimeUnit.MINUTES.toMillis(30));
            }
            if( logLevel == null){
                logLevel = feign.Logger.Level.NONE;
            }
            return new Client( host, logLevel, interceptor, retryer, requestOptions);
        }
    }

    /**
     * Returns an API instance that conforms to the given API Type that can communicate with the Metacat server
     * @param apiType apiType A JAX-RS annotated Metacat interface
     * @param <T> API Resource instance
     * @return An instance that implements the given interface and is wired up to communicate with the Metacat server.
     */
    public <T> T getApiClient(@Nonnull Class<T> apiType) {
        checkArgument(apiType.isInterface(), "apiType must be an interface");

        return feignBuilder.target(apiType, host);
    }

    /**
     *   Return an API instance that can be used to interact with the metacat server
     * @return An instance api conforming to MetacatV1 interface
     */
    public MetacatV1 getApi(){
        return api;
    }

    /**
     *   Return an API instance that can be used to interact with the metacat server for partitions
     * @return An instance api conforming to PartitionV1 interface
     */
    public PartitionV1 getPartitionApi(){
        return partitionApi;
    }

    /**
     *   Return an API instance that can be used to interact with the metacat server for only user metadata
     * @return An instance api conforming to MetadataV1 interface
     */
    public MetadataV1 getMetadataApi(){
        return metadataApi;
    }
}
