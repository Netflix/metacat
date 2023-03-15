package com.netflix.metacat.metadata.store.data.entities;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.hibernate.annotations.GenericGenerator;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Embedded;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.MappedSuperclass;
import javax.persistence.Version;

/**
 * Represents a basic metadata entity.
 *
 * @author rveeramacheneni
 */
@MappedSuperclass
@Getter
@Setter
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(of = "id")
@ToString(of = {
    "id",
    "version",
    "audit"
})
@SuppressWarnings("PMD")
public abstract class BaseEntity {

    @Basic
    @Id
    @GeneratedValue(generator = "uuid")
    @GenericGenerator(name = "uuid", strategy = "uuid2")
    @Column(name = "id", nullable = false, unique = true, updatable = false)
    @Setter(AccessLevel.NONE)
    protected String id;

    @Version
    @Column(name = "version")
    @Setter(AccessLevel.NONE)
    protected Long version;

    @Embedded
    @Builder.Default
    protected AuditEntity audit = new AuditEntity();
}
