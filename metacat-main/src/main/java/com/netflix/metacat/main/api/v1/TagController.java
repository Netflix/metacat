/*
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
 */
package com.netflix.metacat.main.api.v1;

import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.CatalogDto;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.dto.TagCreateRequestDto;
import com.netflix.metacat.common.dto.TagRemoveRequestDto;
import com.netflix.metacat.common.server.connectors.exception.DatabaseNotFoundException;
import com.netflix.metacat.common.server.connectors.exception.TableNotFoundException;
import com.netflix.metacat.common.server.events.MetacatEventBus;
import com.netflix.metacat.common.server.events.MetacatUpdateDatabasePostEvent;
import com.netflix.metacat.common.server.events.MetacatUpdateTablePostEvent;
import com.netflix.metacat.common.server.usermetadata.TagService;
import com.netflix.metacat.common.server.util.MetacatContextManager;
import com.netflix.metacat.main.api.RequestWrapper;
import com.netflix.metacat.main.services.CatalogService;
import com.netflix.metacat.main.services.DatabaseService;
import com.netflix.metacat.main.services.GetTableServiceParameters;
import com.netflix.metacat.main.services.TableService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import lombok.NonNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Nullable;
import java.net.HttpURLConnection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Tag API implementation.
 *
 * @author amajumdar
 */
@RestController
@RequestMapping(
    path = "/mds/v1/tag",
    produces = MediaType.APPLICATION_JSON_VALUE
)
@Api(
    value = "TagV1",
    description = "Federated metadata tag operations",
    produces = MediaType.APPLICATION_JSON_VALUE,
    consumes = MediaType.APPLICATION_JSON_VALUE
)
public class TagController {

    private final RequestWrapper requestWrapper;
    private final TagService tagService;
    private final MetacatEventBus eventBus;
    private final TableService tableService;
    private final DatabaseService databaseService;
    private final CatalogService catalogService;

    /**
     * Constructor.
     *
     * @param eventBus        event bus
     * @param tagService      tag service
     * @param tableService    table service
     * @param databaseService database service
     * @param catalogService  catalog service
     * @param requestWrapper  request wrapper object
     */
    @Autowired
    public TagController(
        final MetacatEventBus eventBus,
        final TagService tagService,
        final TableService tableService,
        final DatabaseService databaseService,
        final CatalogService catalogService,
        final RequestWrapper requestWrapper
    ) {
        this.tagService = tagService;
        this.eventBus = eventBus;
        this.tableService = tableService;
        this.databaseService = databaseService;
        this.catalogService = catalogService;
        this.requestWrapper = requestWrapper;
    }

    /**
     * Return the list of tags.
     *
     * @return list of tags
     */
    @RequestMapping(method = RequestMethod.GET, path = "/tags")
    @ResponseStatus(HttpStatus.OK)
    @ApiOperation(
        position = 1,
        value = "Returns the tags",
        notes = "Returns the tags"
    )
    public Set<String> getTags() {
        return this.requestWrapper.processRequest(
            "TagV1Resource.getTags",
            this.tagService::getTags
        );
    }

    /**
     * Returns the list of qualified names for the given input.
     *
     * @param includeTags  Set of matching tags
     * @param excludeTags  Set of un-matching tags
     * @param sourceName   Prefix of the source name
     * @param databaseName Prefix of the database name
     * @param tableName    Prefix of the table name
     * @return list of qualified names
     */
    @RequestMapping(
        method = RequestMethod.GET,
        path = "/list"
    )
    @ResponseStatus(HttpStatus.OK)
    @ApiOperation(
        position = 1,
        value = "Returns the list of qualified names that are tagged with the given tags."
            + " Qualified names will be excluded if the contained tags matches the excluded tags",
        notes = "Returns the list of qualified names that are tagged with the given tags."
            + " Qualified names will be excluded if the contained tags matches the excluded tags"
    )
    public List<QualifiedName> list(
        @ApiParam(value = "Set of matching tags")
        @Nullable @RequestParam(name = "include", required = false) final Set<String> includeTags,
        @ApiParam(value = "Set of un-matching tags")
        @Nullable @RequestParam(name = "exclude", required = false) final Set<String> excludeTags,
        @ApiParam(value = "Prefix of the source name")
        @Nullable @RequestParam(name = "sourceName", required = false) final String sourceName,
        @ApiParam(value = "Prefix of the database name")
        @Nullable @RequestParam(name = "databaseName", required = false) final String databaseName,
        @ApiParam(value = "Prefix of the table name")
        @Nullable @RequestParam(name = "tableName", required = false) final String tableName
    ) {
        return this.requestWrapper.processRequest(
            "TagV1Resource.list",
            () -> this.tagService.list(includeTags, excludeTags, sourceName, databaseName, tableName)
        );
    }

    /**
     * Returns the list of qualified names that are tagged with tags containing the given tag text.
     *
     * @param tag          Tag partial text
     * @param sourceName   Prefix of the source name
     * @param databaseName Prefix of the database name
     * @param tableName    Prefix of the table name
     * @return list of qualified names
     */
    @RequestMapping(method = RequestMethod.GET, path = "/search")
    @ResponseStatus(HttpStatus.OK)
    @ApiOperation(
        position = 1,
        value = "Returns the list of qualified names that are tagged with tags containing the given tagText",
        notes = "Returns the list of qualified names that are tagged with tags containing the given tagText"
    )
    public List<QualifiedName> search(
        @ApiParam(value = "Tag partial text")
        @Nullable @RequestParam(name = "tag", required = false) final String tag,
        @ApiParam(value = "Prefix of the source name")
        @Nullable @RequestParam(name = "sourceName", required = false) final String sourceName,
        @ApiParam(value = "Prefix of the database name")
        @Nullable @RequestParam(name = "databaseName", required = false) final String databaseName,
        @ApiParam(value = "Prefix of the table name")
        @Nullable @RequestParam(name = "tableName", required = false) final String tableName
    ) {
        return this.requestWrapper.processRequest(
            "TagV1Resource.search",
            () -> tagService.search(tag, sourceName, databaseName, tableName)
        );
    }

    /**
     * Sets the tags on the given object.
     *
     * @param tagCreateRequestDto tag create request dto
     * @return set of tags
     */
    @RequestMapping(
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE
    )
    @ResponseStatus(HttpStatus.CREATED)
    @ApiOperation(
        position = 2,
        value = "Sets the tags on the given resource",
        notes = "Sets the tags on the given resource"
    )
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_CREATED,
                message = "The tags were successfully created"
            ),
            @ApiResponse(
                code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested catalog or database or table cannot be located"
            )
        }
    )
    public Set<String> setTags(
        @ApiParam(value = "Request containing the set of tags and qualifiedName", required = true)
        @RequestBody final TagCreateRequestDto tagCreateRequestDto
    ) {
        return this.requestWrapper.processRequest(
            tagCreateRequestDto.getName(),
            "TagV1Resource.setTags",
            () -> this.setResourceTags(tagCreateRequestDto)
        );
    }

    private Set<String> setResourceTags(@NonNull final TagCreateRequestDto tagCreateRequestDto) {
        final QualifiedName name = tagCreateRequestDto.getName();
        final Set<String> tags = new HashSet<>(tagCreateRequestDto.getTags());
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        if (name.getType().equals(QualifiedName.Type.TABLE)) {
            if (!this.tableService.exists(name)) {
                throw new TableNotFoundException(name);
            }
            final TableDto oldTable = this.tableService
                .get(name, GetTableServiceParameters.builder()
                    .includeInfo(true)
                    .includeDataMetadata(true)
                    .includeDefinitionMetadata(true)
                    .disableOnReadMetadataIntercetor(false)
                    .build())
                .orElseThrow(IllegalStateException::new);
            final Set<String> result = this.tagService.setTags(name, tags, true);
            final TableDto currentTable = this.tableService
                .get(name, GetTableServiceParameters.builder()
                    .includeInfo(true)
                    .includeDataMetadata(true)
                    .includeDefinitionMetadata(true)
                    .disableOnReadMetadataIntercetor(false)
                    .build())
                .orElseThrow(IllegalStateException::new);
            this.eventBus.post(
                new MetacatUpdateTablePostEvent(name, metacatRequestContext, this, oldTable, currentTable)
            );
            return result;
        }
        if (name.getType().equals(QualifiedName.Type.DATABASE)) {
            if (!this.databaseService.exists(name)) {
                throw new DatabaseNotFoundException(name);
            }
            //to do change to set for everything
            final Set<String> result = this.tagService.setTags(name, tags, true);
            this.eventBus.post(
                new MetacatUpdateDatabasePostEvent(name, metacatRequestContext, this)
            );
            return result;
        }
        if (name.getType().equals(QualifiedName.Type.CATALOG)) {
            //catalog service will throw exception if not found
            final CatalogDto catalogDto = this.catalogService.get(name);
            if (catalogDto != null) {
                return this.tagService.setTags(name, tags, true);
            }
        }
        return new HashSet<>();
    }

    /**
     * Sets the tags on the given table.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param tableName    table name
     * @param tags         set of tags
     * @return set of tags
     */
    @RequestMapping(
        method = RequestMethod.POST,
        path = "/catalog/{catalog-name}/database/{database-name}/table/{table-name}",
        consumes = MediaType.APPLICATION_JSON_VALUE
    )
    @ResponseStatus(HttpStatus.CREATED)
    @ApiOperation(
        position = 2,
        value = "Sets the tags on the given table",
        notes = "Sets the tags on the given table"
    )
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_CREATED,
                message = "The tags were successfully created on the table"
            ),
            @ApiResponse(
                code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested catalog or database or table cannot be located"
            )
        }
    )
    public Set<String> setTableTags(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathVariable("catalog-name") final String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathVariable("database-name") final String databaseName,
        @ApiParam(value = "The name of the table", required = true)
        @PathVariable("table-name") final String tableName,
        @ApiParam(value = "Set of tags", required = true)
        @RequestBody final Set<String> tags
    ) {
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        final QualifiedName name = this.requestWrapper.qualifyName(
            () -> QualifiedName.ofTable(catalogName, databaseName, tableName)
        );
        return this.requestWrapper.processRequest(
            name,
            "TagV1Resource.setTableTags",
            () -> {
                // TODO: shouldn't this be in the tag service?
                if (!this.tableService.exists(name)) {
                    throw new TableNotFoundException(name);
                }
                final TableDto oldTable = this.tableService
                    .get(name, GetTableServiceParameters.builder()
                        .includeInfo(true)
                        .includeDataMetadata(true)
                        .includeDefinitionMetadata(true)
                        .disableOnReadMetadataIntercetor(false)
                        .build())
                    .orElseThrow(IllegalStateException::new);
                final Set<String> result = this.tagService.setTags(name, tags, true);
                final TableDto currentTable = this.tableService
                    .get(name, GetTableServiceParameters.builder()
                        .includeInfo(true)
                        .includeDataMetadata(true)
                        .includeDefinitionMetadata(true)
                        .disableOnReadMetadataIntercetor(false)
                        .build())
                    .orElseThrow(IllegalStateException::new);
                this.eventBus.post(
                    new MetacatUpdateTablePostEvent(name, metacatRequestContext, this, oldTable, currentTable)
                );
                return result;
            }
        );
    }

    /**
     * Remove the tags from the given table.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param tableName    table name
     * @param deleteAll    True if all tags need to be removed
     * @param tags         Tags to be removed from the given table
     */
    @RequestMapping(
        method = RequestMethod.DELETE,
        path = "/catalog/{catalog-name}/database/{database-name}/table/{table-name}",
        consumes = MediaType.APPLICATION_JSON_VALUE
    )
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ApiOperation(
        position = 4,
        value = "Remove the tags from the given table",
        notes = "Remove the tags from the given table"
    )
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_NO_CONTENT,
                message = "The tags were successfully deleted from the table"
            ),
            @ApiResponse(
                code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested catalog or database or table cannot be located"
            )
        }
    )
    public void removeTableTags(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathVariable("catalog-name") final String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathVariable("database-name") final String databaseName,
        @ApiParam(value = "The name of the table", required = true)
        @PathVariable("table-name") final String tableName,
        @ApiParam(value = "True if all tags need to be removed")
        @RequestParam(name = "all", defaultValue = "false") final boolean deleteAll,
        @ApiParam(value = "Tags to be removed from the given table")
        @Nullable @RequestBody(required = false) final Set<String> tags
    ) {
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        final QualifiedName name = this.requestWrapper.qualifyName(
            () -> QualifiedName.ofTable(catalogName, databaseName, tableName)
        );
        this.requestWrapper.processRequest(
            name,
            "TagV1Resource.removeTableTags",
            () -> {
                //TODO: Business logic in API tier...
                if (!this.tableService.exists(name)) {
                    // Delete tags if exists
                    this.tagService.delete(name, false);
                    throw new TableNotFoundException(name);
                }
                final TableDto oldTable = this.tableService
                    .get(name, GetTableServiceParameters.builder()
                        .includeInfo(true)
                        .includeDataMetadata(true)
                        .includeDefinitionMetadata(true)
                        .disableOnReadMetadataIntercetor(false)
                        .build())
                    .orElseThrow(IllegalStateException::new);
                this.tagService.removeTags(name, deleteAll, tags, true);
                final TableDto currentTable = this.tableService
                    .get(name, GetTableServiceParameters.builder().includeInfo(true)
                        .includeDataMetadata(true)
                        .includeDefinitionMetadata(true)
                        .disableOnReadMetadataIntercetor(false)
                        .build())
                    .orElseThrow(IllegalStateException::new);

                this.eventBus.post(
                    new MetacatUpdateTablePostEvent(
                        name,
                        metacatRequestContext,
                        this,
                        oldTable,
                        currentTable
                    )
                );
                return null;
            }
        );
    }

    /**
     * Remove the tags from the given resource.
     *
     * @param tagRemoveRequestDto remove tag request dto
     */
    @RequestMapping(
        method = RequestMethod.DELETE,
        consumes = MediaType.APPLICATION_JSON_VALUE
    )
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ApiOperation(
        position = 4,
        value = "Remove the tags from the given resource",
        notes = "Remove the tags from the given resource"
    )
    @ApiResponses(
        {
            @ApiResponse(
                code = HttpURLConnection.HTTP_NO_CONTENT,
                message = "The tags were successfully deleted from the table"
            ),
            @ApiResponse(
                code = HttpURLConnection.HTTP_NOT_FOUND,
                message = "The requested catalog or database or table cannot be located"
            )
        }
    )
    public void removeTags(
        @ApiParam(value = "Request containing the set of tags and qualifiedName", required = true)
        @RequestBody final TagRemoveRequestDto tagRemoveRequestDto
    ) {


        this.requestWrapper.processRequest(
            tagRemoveRequestDto.getName(),
            "TagV1Resource.removeTableTags",
            () -> {
                this.removeResourceTags(tagRemoveRequestDto);
                return null;
            }
        );
    }

    private void removeResourceTags(final TagRemoveRequestDto tagRemoveRequestDto) {
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        final QualifiedName name = tagRemoveRequestDto.getName();

        if (name.getType().equals(QualifiedName.Type.TABLE)) {
            if (!this.tableService.exists(name)) {
                this.tagService.delete(name, false);
                throw new TableNotFoundException(name);
            }
            final TableDto oldTable = this.tableService
                .get(name, GetTableServiceParameters.builder()
                    .includeInfo(true)
                    .includeDataMetadata(true)
                    .includeDefinitionMetadata(true)
                    .disableOnReadMetadataIntercetor(false)
                    .build())
                .orElseThrow(IllegalStateException::new);
            this.tagService.removeTags(name, tagRemoveRequestDto.getDeleteAll(),
                new HashSet<>(tagRemoveRequestDto.getTags()), true);
            final TableDto currentTable = this.tableService
                .get(name, GetTableServiceParameters.builder()
                    .includeInfo(true)
                    .includeDataMetadata(true)
                    .includeDefinitionMetadata(true)
                    .disableOnReadMetadataIntercetor(false)
                    .build())
                .orElseThrow(IllegalStateException::new);
            this.eventBus.post(
                new MetacatUpdateTablePostEvent(name, metacatRequestContext, this, oldTable, currentTable)
            );
        }
        if (name.getType().equals(QualifiedName.Type.DATABASE)) {
            if (!this.databaseService.exists(name)) {
                throw new DatabaseNotFoundException(name);
            }
            //to do change to set for everything
            this.tagService.removeTags(name, tagRemoveRequestDto.getDeleteAll(),
                new HashSet<>(tagRemoveRequestDto.getTags()), true);
            this.eventBus.post(
                new MetacatUpdateDatabasePostEvent(name, metacatRequestContext, this)
            );

        }
        if (name.getType().equals(QualifiedName.Type.CATALOG)) {
            //catalog service will throw exception if not found
            final CatalogDto catalogDto = this.catalogService.get(name);
            if (catalogDto != null) {
                this.tagService.removeTags(name, tagRemoveRequestDto.getDeleteAll(),
                    new HashSet<>(tagRemoveRequestDto.getTags()), true);
            }
        }
    }
}
