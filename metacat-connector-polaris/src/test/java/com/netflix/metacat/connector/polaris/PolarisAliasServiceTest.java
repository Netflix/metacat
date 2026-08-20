package com.netflix.metacat.connector.polaris;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.connector.polaris.store.entities.PolarisTableEntity;
import com.netflix.metacat.connector.polaris.store.repos.PolarisAliasRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for alias resolution against the polaris store.
 */
public class PolarisAliasServiceTest {

    private PolarisAliasRepository aliasRepository;
    private Config config;

    private PolarisAliasService service;
    private QualifiedName alias;
    private QualifiedName source;

    @BeforeEach
    public void setup() {
        aliasRepository = Mockito.mock(PolarisAliasRepository.class);
        config = Mockito.mock(Config.class);
        when(config.isTableAliasEnabled()).thenReturn(true);
        service = new PolarisAliasService(aliasRepository, config);
        alias = QualifiedName.ofTable("prodhive", "db", "the_alias");
        source = QualifiedName.ofTable("prodhive", "db", "the_source");
    }

    private void givenAlias(final String catalog, final String db, final String table) {
        when(aliasRepository.findSourceTable("prodhive", "db", "the_alias"))
            .thenReturn(Optional.of(PolarisTableEntity.builder()
                .catalogName(catalog).dbName(db).tblName(table).build()));
    }

    @Test
    public void testResolvesAliasToSourceTable() {
        givenAlias("prodhive", "db", "the_source");
        assertThat(service.getTableName(alias)).isEqualTo(source);
        assertThat(service.isAlias(alias)).isTrue();
    }

    @Test
    public void testReturnsInputWhenNotAnAlias() {
        when(aliasRepository.findSourceTable("prodhive", "db", "the_alias")).thenReturn(Optional.empty());
        assertThat(service.getTableName(alias)).isEqualTo(alias);
        assertThat(service.isAlias(alias)).isFalse();
    }

    @Test
    public void testResolvesAliasPointingAtAnotherDatabase() {
        givenAlias("prodhive", "other_db", "the_source");
        assertThat(service.getTableName(alias))
            .isEqualTo(QualifiedName.ofTable("prodhive", "other_db", "the_source"));
    }

    @Test
    public void testNonTableNamesAreNotLookedUp() {
        final QualifiedName database = QualifiedName.ofDatabase("prodhive", "db");
        assertThat(service.getTableName(database)).isEqualTo(database);
        assertThat(service.isAlias(database)).isFalse();
        verify(aliasRepository, never()).findSourceTable("prodhive", "db", null);
    }

    @Test
    public void testDoesNotResolveAliasWhenAliasNotEnabled() {
        givenAlias("prodhive", "other_db", "the_source");
        when(config.isTableAliasEnabled()).thenReturn(false);
        assertThat(service.isAlias(QualifiedName.ofTable("prodhive", "other_db", "the_source"))).isFalse();
    }
}
