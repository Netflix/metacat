package com.netflix.metacat.connector.polaris.store.repos;

import jakarta.persistence.EntityManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.Optional;

/**
 * BasePolarisCustomRepository.
 */
public class BasePolarisCustomRepository {
    private final EntityManager defaultEntityManager;
    private final Optional<EntityManager> readerEntityManager;

    /**
     * initialize {@link BasePolarisCustomRepository}.
     *
     * @param defaultEntityManager    - defaultEntityManager
     * @param readerEntityManager - readerEntityManager
     */
    @Autowired
    public BasePolarisCustomRepository(
        final EntityManager defaultEntityManager,
        @Qualifier("readerEntityManager") final Optional<EntityManager> readerEntityManager) {
        this.defaultEntityManager = defaultEntityManager;
        this.readerEntityManager = readerEntityManager;
    }

    protected EntityManager getEntityManager() {
        // Logic to choose which EntityManager to use
        if (readerEntityManager.isPresent()) {
            try {
                Connection connection = readerEntityManager.get().unwrap(Connection.class);
                DatabaseMetaData metaData = connection.getMetaData();
                throw new RuntimeException("hey replica url = " + metaData.getURL());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return readerEntityManager.orElse(defaultEntityManager);
    }
}
