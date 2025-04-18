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
package com.netflix.metacat.main.api.v1;

import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.main.api.RequestWrapper;
import com.netflix.metacat.main.services.search.ElasticSearchUtil;
import io.swagger.v3.oas.annotations.Parameter;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.DependsOn;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Search API.
 */
@ConditionalOnProperty(value = "metacat.elasticsearch.enabled", havingValue = "true")
@RestController
@RequestMapping(
    path = "/mds/v1/search"
)
@DependsOn("metacatCoreInitService")
@RequiredArgsConstructor
public class SearchController {
    private final ElasticSearchUtil elasticSearchUtil;
    private final RequestWrapper requestWrapper;

    /**
     * Searches the list of tables for the given search string.
     *
     * @param searchString search string
     * @return list of tables
     */
    @RequestMapping(method = RequestMethod.GET, path = "/table")
    @ResponseStatus(HttpStatus.OK)
    public List<TableDto> searchTables(
        @Parameter(description = "The query parameter", required = true)
        @RequestParam(name = "q") final String searchString
    ) {
        return this.requestWrapper.processRequest(
            "SearchMetacatV1Resource.searchTables",
            () -> this.elasticSearchUtil.simpleSearch(searchString)
        );
    }
}
