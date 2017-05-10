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

package com.netflix.metacat.main.api;

import com.netflix.metacat.common.api.SearchMetacatV1;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.main.services.search.ElasticSearchUtil;

import javax.inject.Inject;
import java.util.List;

/**
 * Search API.
 */
public class SearchMetacatV1Resource implements SearchMetacatV1 {
    private ElasticSearchUtil elasticSearchUtil;
    private final RequestWrapper requestWrapper;

    /**
     * Constructor.
     * @param elasticSearchUtil search util
     * @param requestWrapper   request wrapper object
     */
    @Inject
    public SearchMetacatV1Resource(final ElasticSearchUtil elasticSearchUtil,
                                   final RequestWrapper requestWrapper) {
        this.elasticSearchUtil = elasticSearchUtil;
        this.requestWrapper = requestWrapper;
    }

    @Override
    public List<TableDto> searchTables(final String searchString) {
        return requestWrapper.processRequest("SearchMetacatV1Resource.searchTables",
            () -> elasticSearchUtil.simpleSearch(searchString));
    }
}
