package com.netflix.metacat.metadata.store.data.entities;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.springframework.data.annotation.CreatedBy;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.LastModifiedBy;
import org.springframework.data.annotation.LastModifiedDate;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Embeddable;
import java.time.Instant;

/**
 * Embeddable entity with audit fields.
 *
 * @author rveeramacheneni
 */
@Embeddable
@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
@ToString(of = {
    "createdBy",
    "lastModifiedBy",
    "createdDate",
    "lastModifiedDate"
})
public class AuditEntity {

    @Basic
    @Column(name = "created_by", nullable = false)
    @CreatedBy
    protected String createdBy;

    @Basic
    @Column(name = "last_updated_by")
    @LastModifiedBy
    protected String lastModifiedBy;

    @Basic
    @Column(name = "created_date", updatable = false)
    @CreatedDate
    protected Instant createdDate;

    @Basic
    @Column(name = "last_updated_date")
    @LastModifiedDate
    protected Instant lastModifiedDate;
}
