package com.netflix.metacat.main.api;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.api.MetadataV1;
import com.netflix.metacat.common.dto.DataMetadataDto;
import com.netflix.metacat.common.dto.DataMetadataGetRequestDto;
import com.netflix.metacat.common.dto.DefinitionMetadataDto;
import com.netflix.metacat.common.dto.SortOrder;
import com.netflix.metacat.common.usermetadata.UserMetadataService;

import javax.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.netflix.metacat.main.api.RequestWrapper.requestWrapper;

/**
 * Created by amajumdar on 6/28/15.
 */
public class MetadataV1Resource implements MetadataV1 {
    private final UserMetadataService userMetadataService;

    @Inject
    public MetadataV1Resource(UserMetadataService userMetadataService) {
        this.userMetadataService = userMetadataService;
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
}
