package com.netflix.metacat.common.server.properties;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.env.Environment;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Parent Child Relationship service properties.
 *
 * @author yingjianw
 */
@Data
@Slf4j
public class ParentChildRelationshipProperties {
    private static final String MAX_ALLOW_PER_TABLE_PER_REL_PROPERTY_NAME =
        "metacat.parentChildRelationshipProperties.maxAllowPerTablePerRelConfig";
    private static final String MAX_ALLOW_PER_DB_PER_REL_PROPERTY_NAME =
        "metacat.parentChildRelationshipProperties.maxAllowPerDBPerRelConfig";
    private static final String DEFAULT_MAX_ALLOW_PER_REL_PROPERTY_NAME =
        "metacat.parentChildRelationshipProperties.defaultMaxAllowPerRelConfig";
    private boolean createEnabled;
    private boolean getEnabled;
    private boolean renameEnabled;
    private boolean dropEnabled;
    private int maxAllow = 5;
    private Map<String, Map<String, Integer>> maxAllowPerTablePerRelType = new HashMap<>();
    private Map<String, Map<String, Integer>> maxAllowPerDBPerRelType = new HashMap<>();
    private Map<String, Integer> defaultMaxAllowPerRelType = new HashMap<>();

    /**
     * Constructor.
     *
     * @param env Spring environment
     */
    public ParentChildRelationshipProperties(@Nullable final Environment env) {
        if (env != null) {
            setMaxAllowPerTablePerRelType(
                env.getProperty(MAX_ALLOW_PER_TABLE_PER_REL_PROPERTY_NAME, String.class, "")
            );
            setMaxAllowPerDBPerRelType(
                env.getProperty(MAX_ALLOW_PER_DB_PER_REL_PROPERTY_NAME, String.class, "")
            );
            setDefaultMaxAllowPerRelType(
                env.getProperty(DEFAULT_MAX_ALLOW_PER_REL_PROPERTY_NAME, String.class, "")
            );
        }
    }

    /**
     * setMaxAllowPerTablePerRelType based on String config.
     *
     * @param  configStr configString
     */
    public void setMaxAllowPerTablePerRelType(@Nullable final String configStr) {
        try {
            this.maxAllowPerTablePerRelType = parseNestedConfigString(configStr);
        } catch (Exception e) {
            log.error("Fail to apply configStr = {} for maxAllowPerTablePerRelType", configStr, e);
        }
    }

    /**
     * setMaxAllowPerDBPerRelType based on String config.
     *
     * @param  configStr configString
     */
    public void setMaxAllowPerDBPerRelType(@Nullable final String configStr) {
        try {
            this.maxAllowPerDBPerRelType = parseNestedConfigString(configStr);
        } catch (Exception e) {
            log.error("Fail to apply configStr = {} for maxCloneAllowPerDBPerRelType", configStr);
        }
    }
    /**
     * setMaxCloneAllowPerDBPerRelType based on String config.
     *
     * @param  configStr configString
     */
    public void setDefaultMaxAllowPerRelType(@Nullable final String configStr) {
        if (configStr == null || configStr.isEmpty()) {
            this.defaultMaxAllowPerRelType = new HashMap<>();
        }
        try {
            this.defaultMaxAllowPerRelType =
                Arrays.stream(configStr.split(";"))
                    .map(entry -> entry.split(","))
                    .collect(Collectors.toMap(
                        parts -> parts[0],
                        parts -> Integer.parseInt(parts[1])
                    ));
        } catch (Exception e) {
            log.error("Fail to apply configStr = {} for defaultMaxAllowPerRelType", configStr);
        }
    }

    private Map<String, Map<String, Integer>> parseNestedConfigString(final String configStr) {
        if (configStr == null || configStr.isEmpty()) {
            return new HashMap<>();
        }
        return Arrays.stream(configStr.split(";"))
            .map(entry -> entry.split(","))
            .collect(Collectors.groupingBy(
                parts -> parts[0],
                Collectors.toMap(
                    parts -> parts[1],
                    parts -> Integer.parseInt(parts[2])
                )
            ));
    }
}
