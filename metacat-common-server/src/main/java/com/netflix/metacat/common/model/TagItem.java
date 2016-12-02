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

package com.netflix.metacat.common.model;

import com.google.common.base.Preconditions;
import com.netflix.metacat.common.server.Config;
import lombok.Data;

import javax.inject.Inject;
import java.util.Date;
import java.util.Set;

/**
 * Tag item details.
 */
@Data
public class TagItem {
    private static Config config;
    private Long id;
    private String name;
    private Set<String> values;
    private Date dateCreated;
    private Date lastUpdated;
    private String createdBy;
    private String lastUpdatedBy;

    /**
     * Constructor.
     */
    public TagItem() {
        Preconditions.checkNotNull(config, "config should have been set in the static setConfig");
        createdBy = config.getLookupServiceUserAdmin();
        lastUpdatedBy = createdBy;
    }

    /**
     * This must be called statically to set the config before the class can be used.
     *
     * @param config the metacat configuration
     */
    @Inject
    public static void setConfig(final Config config) {
        TagItem.config = config;
    }
}
