package com.netflix.metacat.common.server.properties;

import lombok.Data;

/**
 * RateLimiter service properties.
 *
 * @author rveeramacheneni
 */
@Data
public class RateLimiterProperties {
    private boolean enabled;
    private boolean enforced;
}
