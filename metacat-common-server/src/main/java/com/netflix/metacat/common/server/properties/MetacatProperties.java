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

import lombok.Data;
import lombok.NonNull;

/**
 * Entry point to entire property tree of Metacat namespace.
 *
 * @author tgianos
 * @since 1.1.0
 */
@Data
public class MetacatProperties {
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
    private TagServiceProperties tagService = new TagServiceProperties();
    @NonNull
    private ThriftProperties thrift = new ThriftProperties();
    @NonNull
    private TypeProperties type = new TypeProperties();
}
