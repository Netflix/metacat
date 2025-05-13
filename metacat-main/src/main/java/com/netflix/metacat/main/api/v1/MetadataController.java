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

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Sets;
import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.DataMetadataDto;
import com.netflix.metacat.common.dto.DataMetadataGetRequestDto;
import com.netflix.metacat.common.dto.DefinitionMetadataDto;
import com.netflix.metacat.common.dto.SortOrder;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.server.usermetadata.UserMetadataService;
import com.netflix.metacat.common.server.util.MetacatContextManager;
import com.netflix.metacat.main.api.RequestWrapper;
import com.netflix.metacat.main.services.GetTableServiceParameters;
import com.netflix.metacat.main.services.MetacatServiceHelper;
import com.netflix.metacat.main.services.MetadataService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.DependsOn;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Metadata V1 API implementation.
 *
 * @author amajumdar
 */
@RestController
@RequestMapping(
    path = "/mds/v1/metadata"
)
@Tag(
    name = "MetadataV1",
    description = "Federated user metadata operations"
)
@DependsOn("metacatCoreInitService")
@RequiredArgsConstructor
public class MetadataController {
    private final UserMetadataService userMetadataService;
    private final MetacatServiceHelper helper;
    private final MetadataService metadataService;
    private final RequestWrapper requestWrapper;

    /**
     * Returns the data metadata.
     *
     * @param metadataGetRequestDto metadata request
     * @return data metadata
     */
    @RequestMapping(method = RequestMethod.POST, path = "/data")
    @ResponseStatus(HttpStatus.OK)
    @Operation(
        summary = "Returns the data metadata",
        description = "Returns the data metadata"
    )
    public DataMetadataDto getDataMetadata(@RequestBody final DataMetadataGetRequestDto metadataGetRequestDto) {
        return this.requestWrapper.processRequest(
            "getDataMetadata",
            () -> {
                DataMetadataDto result = null;
                if (metadataGetRequestDto.getUri() != null) {
                    final Optional<ObjectNode> o
                        = this.userMetadataService.getDataMetadata(metadataGetRequestDto.getUri());
                    if (o.isPresent()) {
                        result = new DataMetadataDto();
                        result.setDataMetadata(o.get());
                        result.setUri(metadataGetRequestDto.getUri());
                    }
                }
                return result;
            }
        );
    }

    /**
     * Returns the list of definition metadata. Client should be aware that
     * this api does not apply the metadata read interceptor,
     * it queries the original results from database. The definition metadata results from this API can
     * be different from the table get API.
     * TODO: we need to find a way to address the interceptor application or remove this API.
     *
     * @param sortBy         Sort the list by this value
     * @param sortOrder      Sorting order to use
     * @param offset         Offset of the list returned
     * @param limit          Size of the list
     * @param lifetime       has lifetime set
     * @param type           Type of the metadata item. Values: database, table, partition
     * @param name           Text that matches the name of the metadata (accepts sql wildcards)
     * @param dataProperties Set of data property names.
     *                       Filters the returned list that only contains the given property names
     * @return list of definition metadata
     */
    @RequestMapping(method = RequestMethod.GET, path = "/definition/list")
    @ResponseStatus(HttpStatus.OK)
    @Operation(
        summary = "Returns the definition metadata",
        description = "Returns the definition metadata"
    )
    public List<DefinitionMetadataDto> getDefinitionMetadataList(
        @Parameter(description = "Sort the list by this value")
        @Nullable @RequestParam(name = "sortBy", required = false) final String sortBy,
        @Parameter(description = "Sorting order to use")
        @Nullable @RequestParam(name = "sortOrder", required = false) final SortOrder sortOrder,
        @Parameter(description = "Offset of the list returned")
        @Nullable @RequestParam(name = "offset", required = false) final Integer offset,
        @Parameter(description = "Size of the list")
        @Nullable @RequestParam(name = "limit", required = false) final Integer limit,
        @Parameter(description = "has lifetime set")
        @RequestParam(name = "lifetime", defaultValue = "false") final boolean lifetime,
        @Parameter(description = "Type of the metadata item. Values: database, table, partition")
        @Nullable @RequestParam(name = "type", required = false) final String type,
        @Parameter(description = "Text that matches the name of the metadata (accepts sql wildcards)")
        @Nullable @RequestParam(name = "name", required = false) final String name,
        @Parameter(
            description = "Set of data property names. Filters the returned list that only contains the given "
                + "property names"
        )
        @Nullable @RequestParam(name = "data-property", required = false) final Set<String> dataProperties
    ) {
        final Set<String> localDataProperties = dataProperties != null ? dataProperties : Sets.newHashSet();
        if (lifetime) {
            localDataProperties.add("lifetime");
        }

        return requestWrapper.processRequest(
            "getDefinitionMetadataList",
            () -> this.userMetadataService.searchDefinitionMetadata(
                localDataProperties,
                type,
                name,
                getTableDto(name),
                sortBy,
                sortOrder != null ? sortOrder.name() : null,
                offset,
                limit
            )
        );
    }

    private TableDto getTableDto(@Nullable final String name) {
        Optional<TableDto> optionalTableDto = Optional.empty();
        if (name != null) {
            final QualifiedName qualifiedName = QualifiedName.fromString(name);
            if (qualifiedName.isTableDefinition()) {
                optionalTableDto = this.metadataService.getTableService().get(qualifiedName, GetTableServiceParameters
                    .builder().disableOnReadMetadataIntercetor(true)
                    .includeInfo(true)
                    .includeDefinitionMetadata(false)
                    .includeDataMetadata(false)
                    .build());
            }
        }
        return optionalTableDto.isPresent() ? optionalTableDto.get() : null;
    }

    /**
     * Returns the list of qualified names owned by the given owners.
     *
     * @param owners set of owners
     * @return the list of qualified names owned by the given owners
     */
    @RequestMapping(method = RequestMethod.GET, path = "/searchByOwners")
    @ResponseStatus(HttpStatus.OK)
    @Operation(
        summary = "Returns the qualified names owned by the given owners",
        description = "Returns the qualified names owned by the given owners"
    )
    public List<QualifiedName> searchByOwners(
        @Parameter(description = "Set of owners", required = true)
        @RequestParam("owner") final Set<String> owners
    ) {
        return this.requestWrapper.processRequest(
            "searchByOwners",
            () -> userMetadataService.searchByOwners(owners)
        );
    }

    /**
     * Delete the definition metadata for the given name.
     *
     * @param name  Name of definition metadata to be deleted
     * @param force If true, deletes the metadata without checking if the database/table/partition exists
     */
    @RequestMapping(method = RequestMethod.DELETE, path = "/definition")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @Operation(
        summary = "Deletes the given definition metadata"
    )
    public void deleteDefinitionMetadata(
        @Parameter(description = "Name of definition metadata to be deleted", required = true)
        @RequestParam(name = "name") final String name,
        @Parameter(
            description = "If true, deletes the metadata without checking if the database/table/partition exists"
        )
        @RequestParam(name = "force", defaultValue = "false") final boolean force
    ) {
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        requestWrapper.processRequest(
            "deleteDefinitionMetadata",
            () -> {
                metadataService.deleteDefinitionMetadata(QualifiedName.fromString(name), force, metacatRequestContext);
                return null;
            }
        );
    }

    /**
     * Update table.
     *
     * @param catalogName           catalog name
     * @param databaseName          database name
     * @param tableName             table name
     * @param definitionMetadata    definitionMetadata
     *
     */
    @RequestMapping(method = RequestMethod.POST,
        path = "/catalog/{catalog-name}/database/{database-name}/table/{table-name}/definition")
    @ResponseStatus(HttpStatus.OK)
    @Operation(
        summary = "Returns the data metadata",
        description = "Returns the data metadata"
    )
    public void updateDefinitionMetadata(
        @Parameter(description = "The name of the catalog", required = true)
        @PathVariable("catalog-name") final String catalogName,
        @Parameter(description = "The name of the database", required = true)
        @PathVariable("database-name") final String databaseName,
        @Parameter(description = "The name of the table", required = true)
        @PathVariable("table-name") final String tableName,
        @Parameter(description = "The table information", required = true)
        @RequestBody final ObjectNode definitionMetadata
    ) {
        final QualifiedName name = this.requestWrapper.qualifyName(
            () -> QualifiedName.ofTable(catalogName, databaseName, tableName)
        );
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        this.requestWrapper.processRequest(
            "updateDefinitionMetadata",
            () -> {
                this.userMetadataService.updateDefinitionMetadata(name,
                    metacatRequestContext.getUserName(),
                    definitionMetadata,
                    true
                );
                return null;
            }
        );
    }


    /**
     * Deletes the data metadata marked for deletion.
     */
    @RequestMapping(method = RequestMethod.DELETE, path = "/data/cleanup")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @Operation(
        hidden = true,
        summary = "Admin API to delete obsolete data metadata"
    )
    public void cleanUpDeletedDataMetadata() {
        this.metadataService.cleanUpDeletedDataMetadata();
    }

    /**
     * Deletes the obsolete metadata.
     */
    @RequestMapping(method = RequestMethod.DELETE, path = "/definition/cleanup")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @Operation(
        hidden = true,
        summary = "Admin API to delete obsolete metadata"
    )
    public void cleanUpObsoleteMetadata() {
        this.metadataService.cleanUpObsoleteDefinitionMetadata();
    }
}
