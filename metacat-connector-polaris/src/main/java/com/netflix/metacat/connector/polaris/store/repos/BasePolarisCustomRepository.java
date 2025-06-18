package com.netflix.metacat.connector.polaris.store.repos;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import lombok.Getter;
import org.hibernate.Session;
import org.hibernate.engine.spi.SessionImplementor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.orm.jpa.EntityManagerFactoryInfo;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Optional;

/**
 * BasePolarisCustomRepository.
 */
@Getter
public class BasePolarisCustomRepository {
    @PersistenceContext(unitName = "reader")
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
        throw new RuntimeException("Hey = " + retrieveJdbcUrl());
    }

    protected String retrieveJdbcUrl() {
        try {
            EntityManagerFactoryInfo info = (EntityManagerFactoryInfo) entityManager.getEntityManagerFactory();
            Connection connection = info.getDataSource().getConnection();
            return connection.getMetaData().getURL();
        } catch (SQLException e) {
            // Handle exceptions related to SQL
            e.printStackTrace();
            return null;
        }
    }
}
