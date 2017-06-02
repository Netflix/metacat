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
package com.netflix.metacat.main.api.v1;

import com.netflix.metacat.common.dto.ResolveByUriRequestDto;
import com.netflix.metacat.common.dto.ResolveByUriResponseDto;
import com.netflix.metacat.common.exception.MetacatNotFoundException;
import com.netflix.metacat.main.services.PartitionService;
import com.netflix.metacat.main.services.TableService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/**
 * Resolver V1 Implementation as Jersey Resource.
 *
 * @author zhenl
 * @since 1.0.0
 */
@RestController
@RequestMapping(
    path = "/mds/v1/resolver",
    produces = MediaType.APPLICATION_JSON_VALUE
)
@Api(
    value = "ResolverV1",
    description = "Metadata resolver operations",
    produces = MediaType.APPLICATION_JSON_VALUE,
    consumes = MediaType.APPLICATION_JSON_VALUE
)
public class ResolverController {
    private TableService tableService;
    private PartitionService partitionService;

    /**
     * Constructor.
     *
     * @param tableService     table service
     * @param partitionService partition service
     */
    @Autowired
    public ResolverController(final TableService tableService, final PartitionService partitionService) {
        this.tableService = tableService;
        this.partitionService = partitionService;
    }

    /**
     * Gets the qualified name by uri.
     *
     * @param resolveByUriRequestDto resolveByUriRequestDto
     * @param prefixSearch           search by prefix flag
     * @return the qualified name of uri
     */
    @RequestMapping(method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    @ApiOperation(
        value = "Returns the list of qualified names of tables and partitions containing the given URI path",
        notes = "Returns the list of qualified names of tables and partitions containing the given URI path"
    )
    public ResolveByUriResponseDto resolveByUri(
        @ApiParam(value = "do prefix search for URI")
        @RequestParam(name = "prefixSearch", defaultValue = "false") final boolean prefixSearch,
        @RequestBody final ResolveByUriRequestDto resolveByUriRequestDto
    ) {
        final ResolveByUriResponseDto result = new ResolveByUriResponseDto();
        result.setTables(this.tableService.getQualifiedNames(resolveByUriRequestDto.getUri(), prefixSearch));
        result.setPartitions(this.partitionService.getQualifiedNames(resolveByUriRequestDto.getUri(), prefixSearch));
        return result;
    }

    /**
     * Check if the uri used more than once.
     *
     * @param prefixSearch           search by prefix flag
     * @param resolveByUriRequestDto resolveByUriRequestDto
     */
    @RequestMapping(
        method = RequestMethod.POST,
        path = "/isUriUsedMoreThanOnce",
        consumes = MediaType.APPLICATION_JSON_VALUE
    )
    @ResponseStatus(HttpStatus.OK)
    @ApiOperation(
        position = 1,
        value = "Returns status 200 if the given URI is being referred more than once."
            + " Returns status 404 if the given URI is not found or not being referred more than once.",
        notes = "Returns status 200 if the given URI is being referred more than once."
            + " Returns status 404 if the given URI is not found or not being referred more than once.")
    public void isUriUsedMoreThanOnce(
        @ApiParam(value = "do prefix search for URI", defaultValue = "false")
        @RequestParam(name = "prefixSearch", defaultValue = "false") final Boolean prefixSearch,
        @RequestBody final ResolveByUriRequestDto resolveByUriRequestDto
    ) {
        final String uri = resolveByUriRequestDto.getUri();
        int size = this.tableService.getQualifiedNames(uri, prefixSearch).size();
        if (size < 2) {
            size += this.partitionService.getQualifiedNames(uri, prefixSearch).size();
        }
        if (size <= 1) {
            throw new MetacatNotFoundException("URI not found more than once");
        }
    }
}
