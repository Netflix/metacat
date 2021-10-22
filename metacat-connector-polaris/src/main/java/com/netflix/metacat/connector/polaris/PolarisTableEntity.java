package com.netflix.metacat.connector.polaris;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;
import javax.persistence.Version;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * Entity class for Table object.
 */
@Getter
@AllArgsConstructor
@EqualsAndHashCode
@Entity
@ToString(callSuper = true)
@Table(name = "TBLS",
    uniqueConstraints = {
      @UniqueConstraint(columnNames = {"id"}),
      @UniqueConstraint(columnNames = {"db_name", "tbl_name"})
    })
public class PolarisTableEntity {
    @Version
    private Long version;

    @Basic
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id", nullable = false, unique = true, updatable = false)
    private String tblId;

    @Basic
    @Column(name = "db_name", nullable = false, unique = true, updatable = true)
    private final String dbName;

    @Basic
    @Column(name = "tbl_name", nullable = false, unique = true, updatable = false)
    private final String tblName;

    PolarisTableEntity(final String dbName, final String tblName) {
        this.dbName = dbName;
        this.tblName = tblName;
    }
}
