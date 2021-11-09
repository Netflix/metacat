package com.netflix.metacat.metadata.store.data.entities;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.metacat.metadata.store.data.converters.ObjectNodeConverter;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.hibernate.annotations.ColumnDefault;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Convert;
import javax.persistence.MappedSuperclass;

/**
 * Represents a basic user metadata entity.
 *
 * @author rveeramacheneni
 */
@MappedSuperclass
@Getter
@Setter
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
@ToString(callSuper = true, of = {
    "isDeleted"
})
@SuppressWarnings("PMD")
public class BaseUserMetadataEntity extends BaseEntity {

    @Basic
    @Column(name = "is_deleted", nullable = false)
    @ColumnDefault("false")
    protected boolean isDeleted;

    @Basic
    @Column(name = "data", columnDefinition = "jsonb")
    @Convert(converter = ObjectNodeConverter.class)
    protected ObjectNode data;
}
