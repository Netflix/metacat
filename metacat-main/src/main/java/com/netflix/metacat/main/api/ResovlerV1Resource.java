package com.netflix.metacat.main.api;

import com.google.inject.Inject;
import com.netflix.metacat.common.api.ResolverV1;
import com.netflix.metacat.common.dto.ResolveByUriRequestDto;
import com.netflix.metacat.common.dto.ResolveByUriResponseDto;
import com.netflix.metacat.main.services.PartitionService;
import com.netflix.metacat.main.services.TableService;

import javax.ws.rs.core.Response;

/**
 * ResovlerServiceImpl.
 *
 * @author zhenl
 * @since 1.0.0
 */
public class ResovlerV1Resource implements ResolverV1 {
    private TableService tableService;
    private PartitionService partitionService;

    /**
     * ResovlerServiceImpl.
     * @param tableService     table service
     * @param partitionService partition service
     */
    @Inject
    ResovlerV1Resource(final TableService tableService, final PartitionService partitionService) {
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
    public ResolveByUriResponseDto resolveByUri(final Boolean prefixSearch,
                                                final ResolveByUriRequestDto resolveByUriRequestDto) {
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
    public Response isUriUsedMoreThanOnce(final Boolean prefixSearch,
                                          final ResolveByUriRequestDto resolveByUriRequestDto) {
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
