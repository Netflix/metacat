package com.netflix.metacat.connector.polaris.store.entities;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.hibernate.annotations.GenericGenerator;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.EntityListeners;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Version;


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
     * @param dbName    database name
     * @param location  database location.
     * @param createdBy user that created this entity.
     */
    public PolarisDatabaseEntity(final String dbName,
                                 final String location,
                                 final String createdBy) {
        this.dbName = dbName;
        this.location = location;
        this.audit = AuditEntity
                .builder()
                .createdBy(createdBy)
                .lastModifiedBy(createdBy)
                .build();
    }
}
