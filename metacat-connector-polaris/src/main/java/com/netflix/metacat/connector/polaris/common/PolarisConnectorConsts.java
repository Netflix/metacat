package com.netflix.metacat.connector.polaris.common;

/**
 * Polaris connector consts.
 */
public final class PolarisConnectorConsts {

    /**
     * Max number of client-side retries for CRDB txns.
     */
    public static final int MAX_CRDB_TXN_RETRIES = 5;

    /**
     * Default Ctor.
     */
    private PolarisConnectorConsts() {
    }
}
