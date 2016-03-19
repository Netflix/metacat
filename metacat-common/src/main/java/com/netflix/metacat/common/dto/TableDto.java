package com.netflix.metacat.common.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.metacat.common.QualifiedName;
import com.wordnik.swagger.annotations.ApiModel;
import com.wordnik.swagger.annotations.ApiModelProperty;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@ApiModel("Table metadata")
@SuppressWarnings("unused")
public class TableDto extends BaseDto implements HasDataMetadata, HasDefinitionMetadata {
    private static final long serialVersionUID = 5922768252406041451L;
    @ApiModelProperty(value = "Contains information about table changes")
    private AuditDto audit;
    // Marked as transient because we serialize it manually, however as a JsonProperty because Jackson does serialize it
    @ApiModelProperty(value = "metadata attached to the physical data")
    @JsonProperty
    private transient ObjectNode dataMetadata;
    // Marked as transient because we serialize it manually, however as a JsonProperty because Jackson does serialize it
    @ApiModelProperty(value = "metadata attached to the logical table")
    @JsonProperty
    private transient ObjectNode definitionMetadata;
    private List<FieldDto> fields;
    @ApiModelProperty(value = "Any extra metadata properties of the database table")
    private Map<String, String> metadata;
    private QualifiedName name;
    @ApiModelProperty(value = "serialization/deserialization info about the table")
    private StorageDto serde;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TableDto)) return false;
        TableDto tableDto = (TableDto) o;
        return Objects.equals(audit, tableDto.audit) &&
                Objects.equals(dataMetadata, tableDto.dataMetadata) &&
                Objects.equals(definitionMetadata, tableDto.definitionMetadata) &&
                Objects.equals(fields, tableDto.fields) &&
                Objects.equals(metadata, tableDto.metadata) &&
                Objects.equals(name, tableDto.name) &&
                Objects.equals(serde, tableDto.serde);
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
    public void setDataMetadata(ObjectNode dataMetadata) {
        this.dataMetadata = dataMetadata;
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
    public void setDefinitionMetadata(ObjectNode definitionMetadata) {
        this.definitionMetadata = definitionMetadata;
    }

    public List<FieldDto> getFields() {
        return fields;
    }

    public void setFields(List<FieldDto> fields) {
        this.fields = fields;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, String> metadata) {
        this.metadata = metadata;
    }

    @ApiModelProperty(value = "the name of this entity", required = true)
    @JsonProperty
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

    @ApiModelProperty(value = "List of partition key names", required = false)
    @JsonProperty
    public List<String> getPartition_keys() {
        if (fields == null) {
            return null;
        } else if (fields.isEmpty()) {
            return Collections.emptyList();
        }

        List<String> keys = new LinkedList<>();
        for (FieldDto field : fields) {
            if (field.isPartition_key()) {
                keys.add(field.getName());
            }
        }
        return keys;
    }

    @SuppressWarnings("EmptyMethod")
    public void setPartition_keys(List<String> ignored) {
    }

    public StorageDto getSerde() {
        return serde;
    }

    public void setSerde(StorageDto serde) {
        this.serde = serde;
    }

    @Override
    public int hashCode() {
        return Objects.hash(audit, dataMetadata, definitionMetadata, fields, metadata, name, serde);
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
