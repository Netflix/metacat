package com.netflix.metacat.common.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.metacat.common.QualifiedName;
import com.wordnik.swagger.annotations.ApiModelProperty;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.Objects;

@SuppressWarnings("unused")
public class PartitionDto extends BaseDto implements HasDataMetadata, HasDefinitionMetadata {
    private static final long serialVersionUID = 783462697901395508L;
    @ApiModelProperty(value = "audit information about the partition")
    private AuditDto audit;
    // Marked as transient because we serialize it manually, however as a JsonProperty because Jackson does serialize it
    @ApiModelProperty(value = "metadata attached to this partition")
    @JsonProperty
    private transient ObjectNode dataMetadata;
    // Marked as transient because we serialize it manually, however as a JsonProperty because Jackson does serialize it
    @ApiModelProperty(value = "metadata attached to the physical data")
    @JsonProperty
    private transient ObjectNode definitionMetadata;
    @ApiModelProperty(value = "the name of this entity", required = true)
    @JsonProperty
    private QualifiedName name;
    @ApiModelProperty(value = "Storage/Serialization/Deserialization info of the partition ")
    private StorageDto serde;
    @ApiModelProperty(value = "Any extra metadata properties of the partition", required = false)
    private Map<String, String> metadata;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PartitionDto)) return false;
        PartitionDto that = (PartitionDto) o;
        return Objects.equals(audit, that.audit) &&
                Objects.equals(dataMetadata, that.dataMetadata) &&
                Objects.equals(definitionMetadata, that.definitionMetadata) &&
                Objects.equals(name, that.name) &&
                Objects.equals(serde, that.serde) &&
                Objects.equals(metadata, that.metadata);
    }

    public AuditDto getAudit() {
        return audit;
    }

    public void setAudit(AuditDto audit) {
        this.audit = audit;
    }

    @Override
    public ObjectNode getDataMetadata() {
        return dataMetadata;
    }

    @Override
    public void setDataMetadata(ObjectNode metadata) {
        this.dataMetadata = metadata;
    }

    @Nonnull
    @Override
    @JsonIgnore
    public String getDataUri() {
        String uri = serde != null ? serde.getUri() : null;
        if (uri == null || uri.isEmpty()) {
            throw new IllegalStateException("This instance does not have external data");
        }

        return uri;
    }

    @Override
    public ObjectNode getDefinitionMetadata() {
        return definitionMetadata;
    }

    @Override
    public void setDefinitionMetadata(ObjectNode metadata) {
        this.definitionMetadata = metadata;
    }

    public QualifiedName getName() {
        return name;
    }

    public void setName(QualifiedName name) {
        this.name = name;
    }

    @JsonIgnore
    public QualifiedName getDefinitionName() {
        return name;
    }

    public StorageDto getSerde() {
        return serde;
    }

    public void setSerde(StorageDto serde) {
        this.serde = serde;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, String> metadata) {
        this.metadata = metadata;
    }

    @Override
    public int hashCode() {
        return Objects.hash(audit, dataMetadata, definitionMetadata, name, serde, metadata);
    }

    @Override
    @JsonProperty
    public boolean isDataExternal() {
        return serde != null && serde.getUri() != null && !serde.getUri().isEmpty();
    }

    @SuppressWarnings("EmptyMethod")
    public void setDataExternal(boolean ignored) {
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        dataMetadata = deserializeObjectNode(in);
        definitionMetadata = deserializeObjectNode(in);
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        serializeObjectNode(out, dataMetadata);
        serializeObjectNode(out, definitionMetadata);
    }
}
