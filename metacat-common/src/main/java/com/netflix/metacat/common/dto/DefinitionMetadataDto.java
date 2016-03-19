package com.netflix.metacat.common.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.metacat.common.QualifiedName;
import com.wordnik.swagger.annotations.ApiModelProperty;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Created by amajumdar on 5/28/15.
 */
public class DefinitionMetadataDto extends BaseDto implements HasDefinitionMetadata{
    private static final long serialVersionUID = 3826462875655878L;
    private QualifiedName name;
    // Marked as transient because we serialize it manually, however as a JsonProperty because Jackson does serialize it
    @ApiModelProperty(value = "metadata")
    @JsonProperty
    private transient ObjectNode definitionMetadata;

    public QualifiedName getName() {
        return name;
    }

    public void setName(QualifiedName name) {
        this.name = name;
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        definitionMetadata = deserializeObjectNode(in);
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        serializeObjectNode(out, definitionMetadata);
    }

    @Override
    public ObjectNode getDefinitionMetadata() {
        return definitionMetadata;
    }

    @Override
    public void setDefinitionMetadata(ObjectNode definitionMetadata) {
        this.definitionMetadata = definitionMetadata;
    }

    @Override
    @JsonIgnore
    public QualifiedName getDefinitionName() {
        return name;
    }
}
