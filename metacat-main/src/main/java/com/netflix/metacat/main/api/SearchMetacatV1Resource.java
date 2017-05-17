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
package com.netflix.metacat.main.api;

import com.netflix.metacat.common.api.SearchMetacatV1;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.main.services.search.ElasticSearchUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Search API.
 */
@Component
@ConditionalOnProperty(value = "metacat.elasticsearch.enabled", havingValue = "true")
public class SearchMetacatV1Resource implements SearchMetacatV1 {
    private final ElasticSearchUtil elasticSearchUtil;

    /**
     * Constructor.
     *
     * @param elasticSearchUtil search util
     */
    @Autowired
    public SearchMetacatV1Resource(final ElasticSearchUtil elasticSearchUtil) {
        this.elasticSearchUtil = elasticSearchUtil;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<TableDto> searchTables(final String searchString) {
        return RequestWrapper.requestWrapper(
            "SearchMetacatV1Resource.searchTables",
            () -> this.elasticSearchUtil.simpleSearch(searchString)
        );
    }
}
