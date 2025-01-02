package com.netflix.metacat.connector.polaris.store.entities;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.LastModifiedDate;

import jakarta.persistence.Basic;
import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import java.time.Instant;

/**
 * Embeddable audit entity.
 *
 * @author rveeramacheneni
 */
@Embeddable
@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
@ToString(of = {
        "createdBy",
        "lastModifiedBy",
        "createdDate",
        "lastModifiedDate"
})
public class AuditEntity {
    @Basic
    @Column(name = "created_by")
    private String createdBy;

    @Basic
    @Column(name = "last_updated_by")
    private String lastModifiedBy;

    @Basic
    @Column(name = "created_date", updatable = false, nullable = false)
    @CreatedDate
    private Instant createdDate;

    @Basic
    @Column(name = "last_updated_date", nullable = false)
    @LastModifiedDate
    private Instant lastModifiedDate;
}
