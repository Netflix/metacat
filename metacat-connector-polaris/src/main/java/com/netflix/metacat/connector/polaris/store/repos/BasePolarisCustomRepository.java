package com.netflix.metacat.connector.polaris.store.repos;

import jakarta.persistence.EntityManager;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.Optional;

/**
 * BasePolarisCustomRepository.
 */
@Getter
public class BasePolarisCustomRepository {
    private final EntityManager entityManager;

    /**
     * initialize {@link BasePolarisCustomRepository}.
     *
     * @param entityManager - entityManager
     */
    @Autowired
    public BasePolarisCustomRepository(
        final EntityManager entityManager) {
        this.entityManager = entityManager;
    }
}
