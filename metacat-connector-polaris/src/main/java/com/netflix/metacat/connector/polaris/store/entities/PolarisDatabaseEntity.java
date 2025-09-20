package com.netflix.metacat.connector.polaris.store.entities;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
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


/**
 * Entity class for Database object.
 */
@Getter
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
@EqualsAndHashCode
@Entity
@ToString(callSuper = true)
@Table(name = "DBS")
@EntityListeners(AuditingEntityListener.class)
public class PolarisDatabaseEntity {
    @Version
    private Long version;

    @Basic
    @Id
    @GeneratedValue(generator = "uuid")
    @GenericGenerator(name = "uuid", strategy = "uuid2")
    @Column(name = "id", nullable = false, unique = true, updatable = false)
    private String dbId;

    @Basic
    @Column(name = "catalog_name", nullable = false, updatable = false)
    private String catalogName;

    @Basic
    @Column(name = "name", nullable = false, unique = true, updatable = false)
    private String dbName;

    @Basic
    @Column(name = "location", updatable = false)
    private String location;

    @Embedded
    private AuditEntity audit;

    /**
     * Constructor for Polaris Database Entity.
     *
     * @param catalogName catalog name
     * @param dbName    database name
     * @param location  database location.
     * @param createdBy user that created this entity.
     */
    public PolarisDatabaseEntity(
        final String catalogName,
        final String dbName,
                                 final String location,
                                 final String createdBy) {
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.location = location;
        this.audit = AuditEntity
                .builder()
                .createdBy(createdBy)
                .lastModifiedBy(createdBy)
                .build();
    }
}
