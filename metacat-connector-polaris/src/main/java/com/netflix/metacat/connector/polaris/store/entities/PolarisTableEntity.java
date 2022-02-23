package com.netflix.metacat.connector.polaris.store.entities;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.EntityListeners;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Version;


/**
 * Entity class for Table object.
 */
@Getter
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
@EqualsAndHashCode
@Entity
@ToString(callSuper = true)
@Table(name = "TBLS")
@EntityListeners(AuditingEntityListener.class)
public class PolarisTableEntity {
    @Version
    private Long version;

    @Basic
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id", nullable = false, unique = true, updatable = false)
    private String tblId;

    @Basic
    @Column(name = "db_name", nullable = false, updatable = false)
    private String dbName;

    @Basic
    @Setter
    @Column(name = "tbl_name", nullable = false)
    private String tblName;

    @Basic
    @Setter
    @Column(name = "previous_metadata_location", nullable = true, updatable = true)
    private String previousMetadataLocation;

    @Basic
    @Setter
    @Column(name = "metadata_location", nullable = true, updatable = true)
    private String metadataLocation;

    @Embedded
    private AuditEntity audit;

    /**
     * Constructor for Polaris Table Entity.
     *
     * @param dbName    database name
     * @param tblName   table name
     * @param createdBy user that created this entity.
     */
    public PolarisTableEntity(final String dbName,
                              final String tblName,
                              final String createdBy) {
        this.dbName = dbName;
        this.tblName = tblName;
        this.audit = AuditEntity
                .builder()
                .createdBy(createdBy)
                .lastModifiedBy(createdBy)
                .build();
    }
}
