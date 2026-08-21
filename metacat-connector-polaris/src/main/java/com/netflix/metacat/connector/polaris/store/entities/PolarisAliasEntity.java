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

/**
 * Entity class for a table alias.
 */
@Getter
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
@EqualsAndHashCode
@Entity
@ToString(callSuper = true)
@Table(name = "ALIASES")
@EntityListeners(AuditingEntityListener.class)
public class PolarisAliasEntity {
    @Basic
    @Id
    @GeneratedValue(generator = "uuid")
    @GenericGenerator(name = "uuid", strategy = "uuid2")
    @Column(name = "id", nullable = false, unique = true, updatable = false)
    private String aliasId;

    @Basic
    @Column(name = "db_id", nullable = false, updatable = false)
    private String dbId;

    @Basic
    @Column(name = "name", nullable = false, updatable = false)
    private String name;

    @Basic
    @Column(name = "source_tbl_id", nullable = false)
    private String sourceTblId;

    @Embedded
    private AuditEntity audit;
}
