package com.netflix.metacat.common.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.metacat.common.QualifiedName;
import com.wordnik.swagger.annotations.ApiModel;
import com.wordnik.swagger.annotations.ApiModelProperty;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Objects;

/**
 * Information about a catalog
 */
@ApiModel("Information about a catalog")
@SuppressWarnings("unused")
public class CatalogDto extends BaseDto implements HasDefinitionMetadata {
    private static final long serialVersionUID = -5713826608609231492L;

    @ApiModelProperty(value = "a list of the names of the databases that belong to this catalog", required = true)
    private List<String> databases;
    // Marked as transient because we serialize it manually, however as a JsonProperty because Jackson does serialize it
    @ApiModelProperty(value = "metadata attached to the logical catalog")
    @JsonProperty
    private transient ObjectNode definitionMetadata;
    @ApiModelProperty(value = "the name of this entity", required = true)
    @JsonProperty
    private QualifiedName name;
    @ApiModelProperty(value = "the type of the connector of this catalog", required = true)
    private String type;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CatalogDto)) return false;
        CatalogDto that = (CatalogDto) o;
        return Objects.equals(databases, that.databases) &&
                Objects.equals(definitionMetadata, that.definitionMetadata) &&
                Objects.equals(name, that.name) &&
                Objects.equals(type, that.type);
    }

    /**
     * @return the list of the names of the databases under the catalog
     */
    public List<String> getDatabases() {
        return databases;
    }

    /**
     * @param databases the list of the names of the databases under this catalog
     */
    public void setDatabases(List<String> databases) {
        this.databases = databases;
    }

    @Override
    public ObjectNode getDefinitionMetadata() {
        return definitionMetadata;
    }

    @Override
    public void setDefinitionMetadata(ObjectNode metadata) {
        this.definitionMetadata = metadata;
    }

    @JsonIgnore
    public QualifiedName getDefinitionName() {
        return name;
    }

    /**
     * @return name of the catalog
     */
    public QualifiedName getName() {
        return name;
    }

    /**
     * @param name The name of this catalog
     */
    public void setName(QualifiedName name) {
        this.name = name;
    }

    /**
     * @return the name of the connector
     */
    public String getType() {
        return type;
    }

    /**
     * @param type the name of the connector used by this catalog
     */
    public void setType(String type) {
        this.type = type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(databases, definitionMetadata, name, type);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        definitionMetadata = deserializeObjectNode(in);
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        serializeObjectNode(out, definitionMetadata);
    }
}
