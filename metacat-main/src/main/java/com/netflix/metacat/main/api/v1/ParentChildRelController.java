package com.netflix.metacat.main.api.v1;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.notifications.ChildInfoDto;
import com.netflix.metacat.common.server.usermetadata.ParentChildRelMetadataService;
import com.netflix.metacat.main.api.RequestWrapper;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
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
 * @author Yingjianw
 */

@RestController
@RequestMapping(
    path = "/mds/v1/parentChildRel",
    produces = MediaType.APPLICATION_JSON_VALUE
)
@Api(
    value = "ParentChildRelV1",
    description = "Federated user metadata operations",
    produces = MediaType.APPLICATION_JSON_VALUE,
    consumes = MediaType.APPLICATION_JSON_VALUE
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
    @ApiOperation(
        position = 0,
        value = "Returns the children",
        notes = "Returns the children"
    )
    public Set<ChildInfoDto> getChildren(
        @ApiParam(value = "The name of the catalog", required = true)
        @PathVariable("catalog-name") final String catalogName,
        @ApiParam(value = "The name of the database", required = true)
        @PathVariable("database-name") final String databaseName,
        @ApiParam(value = "The name of the table", required = true)
        @PathVariable("table-name") final String tableName
    ) {
        return this.requestWrapper.processRequest(
            "ParentChildRelV1Resource.getChildren",
            () -> this.parentChildRelMetadataService.getChildrenDto(
                QualifiedName.ofTable(catalogName, databaseName, tableName)
            )
        );
    }
}
