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
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.DependsOn;
import org.springframework.http.HttpStatus;
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
    path = "/mds/v1/resolver"
)
@Tag(
    name = "ResolverV1",
    description = "Metadata resolver operations"
)
@DependsOn("metacatCoreInitService")
@RequiredArgsConstructor
public class ResolverController {
    private final TableService tableService;
    private final PartitionService partitionService;

    /**
     * Gets the qualified name by uri.
     *
     * @param resolveByUriRequestDto resolveByUriRequestDto
     * @param prefixSearch           search by prefix flag
     * @return the qualified name of uri
     */
    @RequestMapping(method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.OK)
    @Operation(
        summary = "Returns the list of qualified names of tables and partitions containing the given URI path",
        description = "Returns the list of qualified names of tables and partitions containing the given URI path"
    )
    public ResolveByUriResponseDto resolveByUri(
        @Parameter(description = "do prefix search for URI")
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
        path = "/isUriUsedMoreThanOnce"
    )
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @Operation(
        summary = "Returns status 204 if the given URI is being referred more than once."
            + " Returns status 404 if the given URI is not found or not being referred more than once.",
        description = "Returns status 204 if the given URI is being referred more than once."
            + " Returns status 404 if the given URI is not found or not being referred more than once.")
    public void isUriUsedMoreThanOnce(
        @Parameter(description = "do prefix search for URI")
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
