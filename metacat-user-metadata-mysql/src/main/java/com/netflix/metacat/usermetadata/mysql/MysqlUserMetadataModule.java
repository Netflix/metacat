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

package com.netflix.metacat.usermetadata.mysql;

import com.google.inject.AbstractModule;
import com.netflix.metacat.common.json.MetacatJson;
import com.netflix.metacat.common.usermetadata.LookupService;
import com.netflix.metacat.common.usermetadata.TagService;
import com.netflix.metacat.common.usermetadata.UserMetadataService;
import com.netflix.metacat.common.util.DataSourceManager;

/**
 * Guice module.
 */
public class MysqlUserMetadataModule extends AbstractModule {
    @Override
    protected void configure() {
        requireBinding(MetacatJson.class);
        requireBinding(DataSourceManager.class);
        binder().bind(UserMetadataService.class).to(MysqlUserMetadataService.class).asEagerSingleton();
        binder().bind(LookupService.class).to(MySqlLookupService.class).asEagerSingleton();
        binder().bind(TagService.class).to(MySqlTagService.class).asEagerSingleton();
    }
}
