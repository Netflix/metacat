package com.netflix.metacat.s3.connector.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.UniqueConstraint;

/**
 * Created by amajumdar on 12/23/14.
 */
@Entity
@javax.persistence.Table(name="field",
        uniqueConstraints= @UniqueConstraint(name="field_u1",columnNames = {"schema_id", "name", "pos"}))
public class Field extends IdEntity{
    private int pos;
    private String name;
    private String type;
    private String sourceType;
    private String comment;
    private boolean partitionKey;
    private Schema schema;

    @Column(name="pos", nullable = false)
    public int getPos() {
        return pos;
    }

    public void setPos(int pos) {
        this.pos = pos;
    }

    @Column(name="name", nullable = false)
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Column(name="type", nullable = false, length=4000)
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Column(name="source_type", nullable = true)
    public String getSourceType() {
        return sourceType;
    }

    public void setSourceType(String sourceType) {
        this.sourceType = sourceType;
    }

    @Column(name="comment", nullable = true)
    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    @Column(name="partition_key", nullable = false)
    public boolean isPartitionKey() {
        return partitionKey;
    }

    public void setPartitionKey(boolean partitionKey) {
        this.partitionKey = partitionKey;
    }

    @ManyToOne(fetch = FetchType.LAZY, optional=false)
    @JoinColumn(name = "schema_id", nullable = false)
    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }
}
