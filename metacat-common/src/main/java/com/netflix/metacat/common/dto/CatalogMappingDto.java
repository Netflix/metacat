package com.netflix.metacat.common.dto;

import com.wordnik.swagger.annotations.ApiModel;
import com.wordnik.swagger.annotations.ApiModelProperty;

import java.util.Objects;

@ApiModel(description = "The name and type of a catalog")
@SuppressWarnings("unused")
public class CatalogMappingDto extends BaseDto {
    private static final long serialVersionUID = -1223516438943164936L;

    @ApiModelProperty(value = "The name of the catalog", required = true)
    private String catalogName;
    @ApiModelProperty(value = "The connector type of the catalog", required = true)
    private String connectorName;

    public CatalogMappingDto() {
    }

    public CatalogMappingDto(String catalogName, String connectorName) {
        this.catalogName = catalogName;
        this.connectorName = connectorName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CatalogMappingDto)) return false;
        CatalogMappingDto that = (CatalogMappingDto) o;
        return Objects.equals(catalogName, that.catalogName) &&
                Objects.equals(connectorName, that.connectorName);
    }

    public String getCatalogName() {
        return catalogName;
    }

    public void setCatalogName(String catalogName) {
        this.catalogName = catalogName;
    }

    public String getConnectorName() {
        return connectorName;
    }

    public void setConnectorName(String connectorName) {
        this.connectorName = connectorName;
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalogName, connectorName);
    }

}
