package com.netflix.metacat.connector.polaris.store.entities;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
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

public class PolarisDatabaseEntity {
    @Version
    private Long version;

    @Basic
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id", nullable = false, unique = true, updatable = false)
    private String dbId;

    @Basic
    @Column(name = "name", nullable = false, unique = true, updatable = false)
    private String dbName;

    /**
     * Constructor for Polaris Database Entity.
     * @param dbName database name
     */
    public PolarisDatabaseEntity(final String dbName) {
        this.dbName = dbName;
    }
}
