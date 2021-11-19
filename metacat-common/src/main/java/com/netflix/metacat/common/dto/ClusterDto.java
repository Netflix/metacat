package com.netflix.metacat.common.dto;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Catalog cluster information.
 *
 * @author rveeramacheneni
 * @since 1.3.0
 */
@ApiModel(description = "Information about the catalog cluster")
@SuppressWarnings("unused")
@Data
@EqualsAndHashCode(callSuper = false)
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ClusterDto implements Serializable {
    private static final long serialVersionUID = 3575620733293405903L;
    /** Name of the cluster. */
    @ApiModelProperty(value = "the cluster hosting this catalog", required = false)
    private String name;
    /** Type of the cluster. */
    @ApiModelProperty(value = "the type of the cluster", required = true)
    private String type;
    /** Name of the account under which the cluster was created. Ex: "abc_test" */
    @ApiModelProperty(value = "Name of the account under which the cluster was created.", required = false)
    private String account;
    /** Id of the Account under which the cluster was created. Ex: "abc_test" */
    @ApiModelProperty(value = "Id of the Account under which the cluster was created", required = false)
    private String accountId;
    /** Environment under which the cluster exists. Ex: "prod", "test" */
    @ApiModelProperty(value = "the environment in which the cluster exists", required = false)
    private String env;
    /** Region in which the cluster exists. Ex: "us-east-1" */
    @ApiModelProperty(value = "the region of this cluster", required = false)
    private String region;
}
