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

import com.facebook.presto.spi.NotFoundException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import com.netflix.metacat.common.MetacatContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.api.MetadataV1;
import com.netflix.metacat.common.dto.BaseDto;
import com.netflix.metacat.common.dto.DataMetadataDto;
import com.netflix.metacat.common.dto.DataMetadataGetRequestDto;
import com.netflix.metacat.common.dto.DefinitionMetadataDto;
import com.netflix.metacat.common.dto.HasDefinitionMetadata;
import com.netflix.metacat.common.dto.SortOrder;
import com.netflix.metacat.common.usermetadata.UserMetadataService;
import com.netflix.metacat.common.util.MetacatContextManager;
import com.netflix.metacat.main.services.MetacatService;
import com.netflix.metacat.main.services.MetacatServiceHelper;
import com.netflix.metacat.main.services.MetadataService;

import javax.inject.Inject;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.netflix.metacat.main.api.RequestWrapper.requestWrapper;

/**
 * Created by amajumdar on 6/28/15.
 */
public class MetadataV1Resource implements MetadataV1 {
    private final UserMetadataService userMetadataService;
    private final MetacatServiceHelper helper;
    private final MetadataService metadataService;
    @Inject
    public MetadataV1Resource(UserMetadataService userMetadataService,
            MetacatServiceHelper helper, MetadataService metadataService) {
        this.userMetadataService = userMetadataService;
        this.helper = helper;
        this.metadataService = metadataService;
    }

    @Override
    public DataMetadataDto getDataMetadata(DataMetadataGetRequestDto metadataGetRequestDto) {
        return requestWrapper( "getDataMetadata", () -> {
            DataMetadataDto result = null;
            if( metadataGetRequestDto.getUri() != null){
                Optional<ObjectNode> o = userMetadataService.getDataMetadata(metadataGetRequestDto.getUri());
                if(o.isPresent()){
                    result = new DataMetadataDto();
                    result.setDataMetadata( o.get());
                    result.setUri(metadataGetRequestDto.getUri());
                }
            }
            return result;
        });
    }

    @Override
    public List<DefinitionMetadataDto> getDefinitionMetadataList(
            String sortBy,
            SortOrder sortOrder,
            Integer offset,
            Integer limit,
            Boolean lifetime,
            String type,
            String name,
            Set<String> propertyNames) {
        if(lifetime){
            propertyNames.add("lifetime");
        }
        return requestWrapper( "getDefinitionMetadataList"
                , () -> userMetadataService.searchDefinitionMetadatas(propertyNames, type, name, sortBy, sortOrder!=null?sortOrder.name():null, offset, limit));
    }

    @Override
    public List<QualifiedName> searchByOwners(Set<String> owners) {
        return requestWrapper( "searchByOwners"
                , () -> userMetadataService.searchByOwners(owners));
    }

    @Override
    public void deleteDefinitionMetadata(QualifiedName name, Boolean force) {
        MetacatContext metacatContext = MetacatContextManager.getContext();
        requestWrapper( "deleteDefinitionMetadata",
                () -> {
                    MetacatService service = helper.getService(name);
                    BaseDto dto = null;
                    try {
                        dto = service.get(name);
                    } catch(NotFoundException ignored){}
                    if( (force || dto == null) && !"rds".equalsIgnoreCase(name.getCatalogName())) {
                        helper.postPreUpdateEvent(name, dto, metacatContext);
                        userMetadataService.deleteDefinitionMetadatas(Lists.newArrayList(name));
                        if( dto instanceof  HasDefinitionMetadata) {
                            ((HasDefinitionMetadata) dto).setDefinitionMetadata(null);
                        }
                        helper.postPostUpdateEvent(name, dto, metacatContext);
                    }
                    return null;
                });
    }

    @Override
    public Response processDeletedDataMetadata() {
        metadataService.processDeletedDataMetadata();
        return Response.ok().build();
    }
}
