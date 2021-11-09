package com.netflix.metacat.metadata.store.data.entities;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;

/**
 * Represents a data metadata entity.
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
    "uri"
})
@Table(name = "data_metadata")
public class DataMetadataEntity extends BaseUserMetadataEntity {

    @Basic
    @Column(name = "uri", nullable = false, unique = true)
    private String uri;
}
