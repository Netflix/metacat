package com.netflix.metacat.connector.polaris.common;

import com.netflix.metacat.common.server.connectors.ConnectorRequestContext;
import com.netflix.metacat.common.server.connectors.exception.InvalidMetaException;
import org.apache.commons.lang3.StringUtils;

import java.util.regex.Pattern;

/**
 * Polaris connector utils.
 */
public final class PolarisUtils {

    /**
     * Default metacat user.
     */
    public static final String DEFAULT_METACAT_USER = "metacat_user";

    private static final Pattern VALID_NAME = Pattern.compile("[a-zA-Z0-9_]+");

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

    /**
     * Rejects names containing anything other than letters, digits and underscores.
     * @param name The name to validate.
     */
    public static void validateName(final String name) {
        if (name == null || !VALID_NAME.matcher(name).matches()) {
            throw new InvalidMetaException("Invalid name: " + name, null);
        }
    }
}
