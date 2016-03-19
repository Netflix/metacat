package com.netflix.metacat.common.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.wordnik.swagger.annotations.ApiModelProperty;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Created by amajumdar on 5/5/15.
 */
public class DatabaseCreateRequestDto extends BaseDto{
    private static final long serialVersionUID = 6308417213106650174L;
    // Marked as transient because we serialize it manually, however as a JsonProperty because Jackson does serialize it
    @ApiModelProperty(value = "metadata attached to the physical data")
    @JsonProperty
    private transient ObjectNode definitionMetadata;

    public ObjectNode getDefinitionMetadata() {
        return definitionMetadata;
    }

    public void setDefinitionMetadata(ObjectNode definitionMetadata) {
        this.definitionMetadata = definitionMetadata;
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
