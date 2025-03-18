package com.netflix.metacat.connector.polaris.store.entities;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class StringParamsConverterTest {
    private static final Map<String, String> VALID_MAP_PARAM = new HashMap<>() { {
        put("keyA", "value1");
        put("keyB", "value2");
        put("keyC", "value3");
    } };
    private static final String VALID_STRING_PARAM = "{\"keyA\":\"value1\",\"keyB\":\"value2\",\"keyC\":\"value3\"}";
    private static final String NESTED_STRING_PARAM = "{\"keyA\":{\"keyB\":\"value2\"},\"keyC\":\"value3\"}";

    private final StringParamsConverter converter = new StringParamsConverter();

    @Test
    void validMapToStringConversion() {
        String converted = converter.convertToDatabaseColumn(VALID_MAP_PARAM);
        Assertions.assertEquals(
            VALID_STRING_PARAM,
            converted
        );
    }

    @Test
    void validStringToMapConversion() {
        Map<String, String> converted = converter.convertToEntityAttribute(VALID_STRING_PARAM);
        Assertions.assertEquals(
            VALID_MAP_PARAM,
            converted
        );
    }

    @Test
    void invalidStringToMapConversion() {
        Assertions.assertThrowsExactly(
            RuntimeException.class,
            () -> converter.convertToEntityAttribute(VALID_STRING_PARAM.substring(10)),
            "Error converting params String to Map"
        );

        Assertions.assertThrowsExactly(
            RuntimeException.class,
            () -> converter.convertToEntityAttribute(NESTED_STRING_PARAM),
            "Error converting params String to Map"
        );
    }

    @Test
    void nullEmptyConversionBehavior() {
        Assertions.assertNull(converter.convertToDatabaseColumn(null));
        Assertions.assertNull(converter.convertToDatabaseColumn(new HashMap<>()));

        Assertions.assertEquals(new HashMap<>(), converter.convertToEntityAttribute(null));
        Assertions.assertEquals(new HashMap<>(), converter.convertToEntityAttribute(""));
    }
}
