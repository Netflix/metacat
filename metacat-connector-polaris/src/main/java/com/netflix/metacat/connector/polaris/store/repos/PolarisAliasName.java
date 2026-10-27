package com.netflix.metacat.connector.polaris.store.repos;

/**
 * Fully qualified name of a table alias.
 *
 * @param catalog catalog
 * @param db      database
 * @param table   alias name
 */
public record PolarisAliasName(String catalog, String db, String table) {}
