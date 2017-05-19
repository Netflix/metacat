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

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.api.MetadataV1;
import com.netflix.metacat.common.dto.BaseDto;
import com.netflix.metacat.common.dto.DataMetadataDto;
import com.netflix.metacat.common.dto.DataMetadataGetRequestDto;
import com.netflix.metacat.common.dto.DefinitionMetadataDto;
import com.netflix.metacat.common.dto.HasDefinitionMetadata;
import com.netflix.metacat.common.dto.SortOrder;
import com.netflix.metacat.common.server.connectors.exception.NotFoundException;
import com.netflix.metacat.common.server.usermetadata.UserMetadataService;
import com.netflix.metacat.common.server.util.MetacatContextManager;
import com.netflix.metacat.main.services.MetacatService;
import com.netflix.metacat.main.services.MetacatServiceHelper;
import com.netflix.metacat.main.services.MetadataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Metadata V1 API implementation.
 *
 * @author amajumdar
 */
@Component
public class MetadataV1Resource implements MetadataV1 {
    private final UserMetadataService userMetadataService;
    private final MetacatServiceHelper helper;
    private final MetadataService metadataService;
    private final RequestWrapper requestWrapper;

    /**
     * Constructor.
     *
     * @param userMetadataService user metadata service
     * @param helper              helper
     * @param metadataService     metadata service
     * @param requestWrapper      request wrapper object
     */
    @Autowired
    public MetadataV1Resource(final UserMetadataService userMetadataService,
                              final MetacatServiceHelper helper,
                              final MetadataService metadataService,
                              final RequestWrapper requestWrapper) {
        this.userMetadataService = userMetadataService;
        this.helper = helper;
        this.metadataService = metadataService;
        this.requestWrapper = requestWrapper;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataMetadataDto getDataMetadata(final DataMetadataGetRequestDto metadataGetRequestDto) {
        return requestWrapper.processRequest("getDataMetadata", () -> {
            DataMetadataDto result = null;
            if (metadataGetRequestDto.getUri() != null) {
                final Optional<ObjectNode> o = userMetadataService.getDataMetadata(metadataGetRequestDto.getUri());
                if (o.isPresent()) {
                    result = new DataMetadataDto();
                    result.setDataMetadata(o.get());
                    result.setUri(metadataGetRequestDto.getUri());
                }
            }
            return result;
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<DefinitionMetadataDto> getDefinitionMetadataList(
            final String sortBy,
            final SortOrder sortOrder,
            final Integer offset,
            final Integer limit,
            final Boolean lifetime,
            final String type,
            final String name,
            final Set<String> propertyNames) {
        if (lifetime) {
            propertyNames.add("lifetime");
        }
        return requestWrapper.processRequest("getDefinitionMetadataList",
                () -> userMetadataService.searchDefinitionMetadatas(propertyNames, type, name, sortBy,
                        sortOrder != null ? sortOrder.name() : null, offset, limit));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<QualifiedName> searchByOwners(final Set<String> owners) {
        return requestWrapper.processRequest("searchByOwners",
                () -> userMetadataService.searchByOwners(owners));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteDefinitionMetadata(final QualifiedName name, final Boolean force) {
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        requestWrapper.processRequest("deleteDefinitionMetadata",
                () -> {
                    final MetacatService service = helper.getService(name);
                    BaseDto dto = null;
                    try {
                        dto = service.get(name);
                    } catch (NotFoundException ignored) {
                    }
                    if ((force || dto == null) && !"rds".equalsIgnoreCase(name.getCatalogName())) {
                        helper.postPreUpdateEvent(name, metacatRequestContext, dto);
                        userMetadataService.deleteDefinitionMetadatas(Lists.newArrayList(name));
                        if (dto instanceof HasDefinitionMetadata) {
                            ((HasDefinitionMetadata) dto).setDefinitionMetadata(null);
                        }
                        final BaseDto newDto = service.get(name);
                        helper.postPostUpdateEvent(name, metacatRequestContext, dto, newDto);
                    }
                    return null;
                });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Response processDeletedDataMetadata() {
        metadataService.processDeletedDataMetadata();
        return Response.ok().build();
    }
}
