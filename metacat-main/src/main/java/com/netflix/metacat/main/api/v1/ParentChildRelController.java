package com.netflix.metacat.main.api.v1;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.ChildInfoDto;
import com.netflix.metacat.common.dto.ParentInfoDto;
import com.netflix.metacat.common.server.usermetadata.ParentChildRelMetadataService;
import com.netflix.metacat.main.api.RequestWrapper;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.DependsOn;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.PathVariable;

import java.util.Set;

/**
 * Parent Child Relation V1 API implementation.
 *
 * @author Yingjian
 */

@RestController
@RequestMapping(
    path = "/mds/v1/parentChildRel"
)
@Tag(
    name = "ParentChildRelV1",
    description = "Federated user metadata operations"
)
@DependsOn("metacatCoreInitService")
@RequiredArgsConstructor
public class ParentChildRelController {
    private final RequestWrapper requestWrapper;
    private final ParentChildRelMetadataService parentChildRelMetadataService;

    /**
     * Return the list of children for a given table.
     * @param catalogName catalogName
     * @param databaseName databaseName
     * @param tableName tableName
     * @return list of childInfoDto
     */
    @RequestMapping(method = RequestMethod.GET,
        path = "/children/catalog/{catalog-name}/database/{database-name}/table/{table-name}")
    @ResponseStatus(HttpStatus.OK)
    @Operation(
        summary = "Returns the children",
        description = "Returns the children"
    )
    public Set<ChildInfoDto> getChildren(
        @Parameter(description = "The name of the catalog", required = true)
        @PathVariable("catalog-name") final String catalogName,
        @Parameter(description = "The name of the database", required = true)
        @PathVariable("database-name") final String databaseName,
        @Parameter(description = "The name of the table", required = true)
        @PathVariable("table-name") final String tableName
    ) {
        return this.requestWrapper.processRequest(
            "ParentChildRelV1Resource.getChildren",
            () -> this.parentChildRelMetadataService.getChildrenDto(
                QualifiedName.ofTable(catalogName, databaseName, tableName)
            )
        );
    }

    /**
     * Return the list of parent for a given table.
     * @param catalogName catalogName
     * @param databaseName databaseName
     * @param tableName tableName
     * @return list of parentInfoDto
     */
    @RequestMapping(method = RequestMethod.GET,
        path = "/parents/catalog/{catalog-name}/database/{database-name}/table/{table-name}")
    @ResponseStatus(HttpStatus.OK)
    @Operation(
        summary = "Returns the parents",
        description = "Returns the parents"
    )
    public Set<ParentInfoDto> getParents(
        @Parameter(description = "The name of the catalog", required = true)
        @PathVariable("catalog-name") final String catalogName,
        @Parameter(description = "The name of the database", required = true)
        @PathVariable("database-name") final String databaseName,
        @Parameter(description = "The name of the table", required = true)
        @PathVariable("table-name") final String tableName
    ) {
        return this.requestWrapper.processRequest(
            "ParentChildRelV1Resource.getParents",
            () -> this.parentChildRelMetadataService.getParentsDto(
                QualifiedName.ofTable(catalogName, databaseName, tableName)
            )
        );
    }
}
