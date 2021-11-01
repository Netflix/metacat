package com.netflix.metacat.connector.polaris.store.entities;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
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
    private final String dbName;

    /**
     * Constructor for Polaris Database Entity.
     * @param dbName database name
     */
    public PolarisDatabaseEntity(final String dbName) {
        this.dbName = dbName;
    }
}
