/*
 *
 *  Copyright 2017 Netflix, Inc.
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
package com.netflix.metacat.common.server.properties;

import lombok.NonNull;
import org.springframework.core.env.Environment;

/**
 * Entry point to entire property tree of Metacat namespace.
 *
 * @author tgianos
 * @since 1.1.0
 */
@lombok.Data
public class MetacatProperties {
    @NonNull
    private Environment env;
    @NonNull
    private Data data = new Data();
    @NonNull
    private Definition definition = new Definition();
    @NonNull
    private ElasticsearchProperties elasticsearch = new ElasticsearchProperties();
    @NonNull
    private EventProperties event = new EventProperties();
    @NonNull
    private FranklinProperties franklin = new FranklinProperties();
    @NonNull
    private HiveProperties hive = new HiveProperties();
    @NonNull
    private LookupServiceProperties lookupService = new LookupServiceProperties();
    @NonNull
    private NotificationsProperties notifications = new NotificationsProperties();
    @NonNull
    private PluginProperties plugin = new PluginProperties();
    @NonNull
    private ServiceProperties service = new ServiceProperties();
    @NonNull
    private Table table = new Table();
    @NonNull
    private TagServiceProperties tagService = new TagServiceProperties();
    @NonNull
    private ThriftProperties thrift = new ThriftProperties();
    @NonNull
    private TypeProperties type = new TypeProperties();
    @NonNull
    private User user = new User();
    @NonNull
    private UserMetadata usermetadata = new UserMetadata();
    @NonNull
    private CacheProperties cache = new CacheProperties();
    @NonNull
    private AuthorizationServiceProperties authorization = new AuthorizationServiceProperties();
    @NonNull
    private AliasServiceProperties aliasServiceProperties = new AliasServiceProperties();
    @NonNull
    private RateLimiterProperties rateLimiterProperties = new RateLimiterProperties();
    @NonNull
    private ParentChildRelationshipProperties parentChildRelationshipProperties;

    /**
     * Constructor for MetacatProperties.
     *
     * @param env Spring Environment
     */
    public MetacatProperties(final Environment env) {
        this.env = env;
        this.parentChildRelationshipProperties = new ParentChildRelationshipProperties(env);
    }

    /**
     * Constructor for MetacatProperties.
     *
     * @param env Spring Environment
     * @param isAuroraEnabled isAuroraEnabled
     */
    public MetacatProperties(final Environment env, final boolean isAuroraEnabled) {
        this.env = env;
        this.parentChildRelationshipProperties = new ParentChildRelationshipProperties(env);
        this.getService().setAuroraDataSourceEnabled(isAuroraEnabled);
    }
}
