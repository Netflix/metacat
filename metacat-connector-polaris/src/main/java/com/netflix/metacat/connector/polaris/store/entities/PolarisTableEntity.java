package com.netflix.metacat.connector.polaris.store.entities;

import jakarta.persistence.Convert;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.hibernate.annotations.GenericGenerator;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

import jakarta.persistence.Basic;
import jakarta.persistence.Column;
import jakarta.persistence.Embedded;
import jakarta.persistence.Entity;
import jakarta.persistence.EntityListeners;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import java.util.Map;


/**
 * Entity class for Table object.
 */
@Getter
@Setter
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
    @GeneratedValue(generator = "uuid")
    @GenericGenerator(name = "uuid", strategy = "uuid2")
    @Column(name = "id", nullable = false, unique = true, updatable = false)
    private String tblId;

    @Basic
    @Column(name = "catalog_name", nullable = false, updatable = false)
    private String catalogName;

    @Basic
    @Column(name = "db_name", nullable = false, updatable = false)
    private String dbName;

    @Basic
    @Column(name = "tbl_name", nullable = false)
    private String tblName;

    @Basic
    @Column(name = "previous_metadata_location", nullable = true, updatable = true)
    private String previousMetadataLocation;

    @Basic
    @Column(name = "metadata_location", nullable = true, updatable = true)
    private String metadataLocation;

    @Embedded
    private AuditEntity audit;

    @Convert(converter = StringParamsConverter.class)
    @Column(name = "params", nullable = true, updatable = true)
    private Map<String, String> params;

    /**
     * Constructor for Polaris Table Entity.
     *
     * @param catalogName catalog name
     * @param dbName    database name
     * @param tblName   table name
     * @param createdBy user that created this entity.
     */
    public PolarisTableEntity(
         final String catalogName,
                              final String dbName,
                              final String tblName,
                              final String createdBy) {
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.tblName = tblName;
        this.audit = AuditEntity
                .builder()
                .createdBy(createdBy)
                .lastModifiedBy(createdBy)
                .build();
    }
}
