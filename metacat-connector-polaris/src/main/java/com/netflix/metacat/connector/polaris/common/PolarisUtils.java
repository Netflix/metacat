package com.netflix.metacat.connector.polaris.common;

import com.netflix.metacat.common.server.connectors.ConnectorRequestContext;
import org.apache.commons.lang3.StringUtils;

/**
 * Polaris connector utils.
 */
public final class PolarisUtils {

    /**
     * Default metacat user.
     */
    public static final String DEFAULT_METACAT_USER = "metacat_user";

    /**
     * Default Ctor.
     */
    private PolarisUtils() {
    }

    /**
     * Get the user name from the request context or
     * a default one if missing.
     * @param context The request context.
     * @return the user name.
     */
    public static String getUserOrDefault(final ConnectorRequestContext context) {
        final String userName = context.getUserName();
        return StringUtils.isNotBlank(userName) ? userName : DEFAULT_METACAT_USER;
    }
}
