package com.netflix.metacat.metadata.store.data.entities;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.metadata.store.data.converters.QualifiedNameConverter;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Convert;
import javax.persistence.Entity;
import javax.persistence.Table;

/**
 * The definition metadata entity.
 *
 * @author rveeramacheneni
 */
@Entity
@Getter
@Setter
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
@ToString(of = {
    "name"
})
@Table(name = "definition_metadata")
@SuppressWarnings("PMD")
public class DefinitionMetadataEntity extends BaseUserMetadataEntity {

    @Basic
    @Column(name = "name", nullable = false, unique = true)
    @Convert(converter = QualifiedNameConverter.class)
    private QualifiedName name;
}
