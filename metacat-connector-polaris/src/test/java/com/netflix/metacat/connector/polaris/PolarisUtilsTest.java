package com.netflix.metacat.connector.polaris;

import com.netflix.metacat.common.server.connectors.exception.InvalidMetaException;
import com.netflix.metacat.connector.polaris.common.PolarisUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class PolarisUtilsTest {
    @ParameterizedTest
    @ValueSource(strings = {"table", "my_table_1", "_MyTable"})
    public void testValidNames(final String name) {
        assertThatCode(() -> PolarisUtils.validateName(name)).doesNotThrowAnyException();
    }

    @ParameterizedTest
    @ValueSource(strings = {"my-table", "my table", "my.table", "my$table", ""})
    public void testInvalidNames(final String name) {
        assertThatThrownBy(() -> PolarisUtils.validateName(name)).isInstanceOf(InvalidMetaException.class);
    }

    @Test
    public void testNullName() {
        assertThatThrownBy(() -> PolarisUtils.validateName(null)).isInstanceOf(InvalidMetaException.class);
    }
}
