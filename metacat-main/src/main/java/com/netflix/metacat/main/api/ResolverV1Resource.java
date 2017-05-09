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
package com.netflix.metacat.main.api;

import com.netflix.metacat.common.api.ResolverV1;
import com.netflix.metacat.common.dto.ResolveByUriRequestDto;
import com.netflix.metacat.common.dto.ResolveByUriResponseDto;
import com.netflix.metacat.main.services.PartitionService;
import com.netflix.metacat.main.services.TableService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.ws.rs.core.Response;

/**
 * Resolver V1 Implementation as Jersey Resource.
 *
 * @author zhenl
 * @since 1.0.0
 */
@Component
public class ResolverV1Resource implements ResolverV1 {
    private TableService tableService;
    private PartitionService partitionService;

    /**
     * Constructor.
     *
     * @param tableService     table service
     * @param partitionService partition service
     */
    @Autowired
    public ResolverV1Resource(final TableService tableService, final PartitionService partitionService) {
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
    @Override
    public ResolveByUriResponseDto resolveByUri(
        final Boolean prefixSearch,
        final ResolveByUriRequestDto resolveByUriRequestDto
    ) {
        final ResolveByUriResponseDto result = new ResolveByUriResponseDto();
        result.setTables(tableService.getQualifiedNames(resolveByUriRequestDto.getUri(), prefixSearch));
        result.setPartitions(partitionService.getQualifiedNames(resolveByUriRequestDto.getUri(), prefixSearch));
        return result;
    }

    /**
     * Check if the uri used more than once.
     *
     * @param prefixSearch           search by prefix flag
     * @param resolveByUriRequestDto resolveByUriRequestDto
     * @return true if the uri used more than once
     */
    @Override
    public Response isUriUsedMoreThanOnce(
        final Boolean prefixSearch,
        final ResolveByUriRequestDto resolveByUriRequestDto
    ) {
        int size = tableService.getQualifiedNames(resolveByUriRequestDto.getUri(), prefixSearch).size();
        if (size < 2) {
            size += partitionService.getQualifiedNames(resolveByUriRequestDto.getUri(), prefixSearch).size();
        }
        if (size > 1) {
            return Response.ok().build();
        } else {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
    }
}
