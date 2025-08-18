package com.netflix.metacat.connector.polaris.store.routing;

/**
 * DataSourceContextHolder threadlocal to control the datasource.
 */
public final class DataSourceContextHolder {
    private static final ThreadLocal<DataSourceEnum> CONTEXT_HOLDER = new ThreadLocal<>();

    private DataSourceContextHolder() {

    }

    /**
     * Sets the current datasource context for the current thread.
     *
     * @param dataSourceEnum the {@link DataSourceEnum} to be set (e.g., PRIMARY or REPLICA)
     */
    public static void setContext(final DataSourceEnum dataSourceEnum) {
        CONTEXT_HOLDER.set(dataSourceEnum);
    }

    /**
     * Retrieves the current datasource context for the current thread.
     *
     * @return the current {@link DataSourceEnum}, or {@code null} if none is set
     */
    public static DataSourceEnum getContext() {
        return CONTEXT_HOLDER.get();
    }

    /**
     * Clears the current datasource context for the current thread.
     */
    public static void clearContext() {
        CONTEXT_HOLDER.remove();
    }
}
