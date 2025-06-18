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
            throw new RuntimeException("readerEntityManager is present");
//            try {
//                Connection readConnection = readerEntityManager.get().unwrap(Connection.class);
//                DatabaseMetaData readMetaData = readConnection.getMetaData();
//
//                Connection primaryConnection = defaultEntityManager.unwrap(Connection.class);
//                DatabaseMetaData primaryMetaData = primaryConnection.getMetaData();
//                throw new RuntimeException(
//                        "hey replica url = " + readMetaData.getURL() + " primary url = " + primaryMetaData.getURL());
//            } catch (Exception e) {
//                throw new RuntimeException(e);
//            }
        }
        return readerEntityManager.orElse(defaultEntityManager);
    }
}
