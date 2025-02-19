package com.netflix.metacat.common.dto;

import io.swagger.v3.oas.annotations.media.Schema;
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
@Schema(description = "Information about the catalog cluster")
@SuppressWarnings("unused")
@Data
@EqualsAndHashCode(callSuper = false)
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ClusterDto implements Serializable {
    private static final long serialVersionUID = 3575620733293405903L;
    /** Name of the cluster. */
    @Schema(
        description = "the cluster hosting this catalog",
        requiredMode = Schema.RequiredMode.NOT_REQUIRED
    )
    private String name;
    /** Type of the cluster. */
    @Schema(
        description = "the type of the cluster",
        requiredMode = Schema.RequiredMode.REQUIRED
    )
    private String type;
    /** Name of the account under which the cluster was created. Ex: "abc_test" */
    @Schema(
        description = "Name of the account under which the cluster was created.",
        requiredMode = Schema.RequiredMode.NOT_REQUIRED
    )
    private String account;
    /** Id of the Account under which the cluster was created. Ex: "abc_test" */
    @Schema(
        description = "Id of the Account under which the cluster was created",
        requiredMode = Schema.RequiredMode.NOT_REQUIRED
    )
    private String accountId;
    /** Environment under which the cluster exists. Ex: "prod", "test" */
    @Schema(
        description = "the environment in which the cluster exists",
        requiredMode = Schema.RequiredMode.NOT_REQUIRED
    )
    private String env;
    /** Region in which the cluster exists. Ex: "us-east-1" */
    @Schema(
        description = "the region of this cluster",
        requiredMode = Schema.RequiredMode.NOT_REQUIRED
    )
    private String region;
}
