package com.netflix.metacat.usermetadata.mysql;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.netflix.metacat.common.model.Lookup;
import com.netflix.metacat.common.server.Config;
import com.netflix.metacat.common.usermetadata.LookupService;
import com.netflix.metacat.common.usermetadata.UserMetadataServiceException;
import com.netflix.metacat.common.util.DataSourceManager;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Set;

import static com.netflix.servo.util.Preconditions.checkNotNull;

/**
 * Created by amajumdar on 11/19/14.
 */
public class MySqlLookupService implements LookupService{
    private static final Logger log = LoggerFactory.getLogger(MySqlLookupService.class);
    private static final String SQL_GET_LOOKUP = "select id, name, type, created_by createdBy, last_updated_by lastUpdatedBy, date_created dateCreated, last_updated lastUpdated from lookup where name=?";
    private static final String SQL_INSERT_LOOKUP = "insert into loookup( name, version, type, created_by, last_updated_by, date_created, last_updated) values (?,0,?,?,?,now(),now())";
    private static final String SQL_INSERT_LOOKUP_VALUES = "insert into lookup_values( lookup_id, values_string) values (?,?)";
    private static final String SQL_DELETE_LOOKUP_VALUES = "delete from lookup_values where lookup_id=? and values_string in (%s)";
    private static final String SQL_GET_LOOKUP_VALUES = "select values_string value from lookup_values where lookup_id=?";
    private static final String SQL_GET_LOOKUP_VALUES_BY_NAME = "select lv.values_string value from lookup l, lookup_values lv where l.id=lv.lookup_id and l.name=?";
    private static final String STRING_TYPE = "string";
    private final Config config;
    private final DataSourceManager dataSourceManager;

    @Inject
    public MySqlLookupService(Config config, DataSourceManager dataSourceManager){
        this.config = checkNotNull(config, "config is required");
        this.dataSourceManager = checkNotNull(dataSourceManager, "dataSourceManager is required");
    }

    private DataSource getDataSource(){
        return dataSourceManager.get(MysqlUserMetadataService.NAME_DATASOURCE);
    }

    /**
     * Returns the lookup for the given <code>name</code>
     * @param name lookup name
     * @return lookup
     */
    public Lookup get(String name) {
        Lookup result = null;
        try{
            QueryRunner runner = new QueryRunner(getDataSource());
            ResultSetHandler<Lookup> handler = new BeanHandler<>(Lookup.class);
            result =  runner.query(SQL_GET_LOOKUP, handler, name);
            if( result != null){
                result.setValues(getValues(result.getId()));
            }
        } catch(Exception e){
            String message = String.format("Failed to get the lookup for name %s", name);
            log.error( message, e);
            throw new UserMetadataServiceException( message, e);
        }
        return result;
    }

    /**
     * Returns the value of the lookup name
     * @param name lookup name
     * @return scalar lookup value
     */
    public String getValue(String name) {
        String result = null;
        Set<String> values = getValues( name);
        if( values != null && values.size() > 0){
            result = values.iterator().next();
        }
        return result;
    }

    /**
     * Returns the list of values of the lookup name
     * @param lookupId lookup id
     * @return list of lookup values
     */
    public Set<String> getValues(Long lookupId) {
        try{
            QueryRunner runner = new QueryRunner(getDataSource());
            return runner.query(SQL_GET_LOOKUP_VALUES, rs -> {
                Set<String> result = Sets.newHashSet();
                while(rs.next()){
                    result.add( rs.getString("value"));
                }
                return result;
            }, lookupId);
        } catch(Exception e){
            String message = String.format("Failed to get the lookup values for id %s", lookupId);
            log.error( message, e);
            throw new UserMetadataServiceException( message, e);
        }
    }

    /**
     * Returns the list of values of the lookup name
     * @param name lookup name
     * @return list of lookup values
     */
    public Set<String> getValues(String name) {
        try{
            QueryRunner runner = new QueryRunner(getDataSource());
            return runner.query(SQL_GET_LOOKUP_VALUES_BY_NAME, rs -> {
                Set<String> result = Sets.newHashSet();
                while(rs.next()){
                    result.add( rs.getString("value"));
                }
                return result;
            }, name);
        } catch(Exception e){
            String message = String.format("Failed to get the lookup values for name %s", name);
            log.error( message, e);
            throw new UserMetadataServiceException( message, e);
        }
    }

    /**
     * Saves the lookup value
     * @param name lookup name
     * @param values multiple values
     * @return
     */
    public Lookup setValues(String name, Set<String> values) {
        Lookup lookup = null;
        try{
            Connection conn = getDataSource().getConnection();
            try {
                lookup = findOrCreateLookupByName( name, conn);
                Set<String> inserts = Sets.newHashSet();
                Set<String> deletes = Sets.newHashSet();
                Set<String> lookupValues = lookup.getValues();
                if( lookupValues == null || lookupValues.isEmpty()){
                    inserts = values;
                } else {
                    inserts = Sets.difference(values, lookupValues).immutableCopy();
                    deletes = Sets.difference(lookupValues, values).immutableCopy();
                }
                lookup.setValues( values);
                if( !inserts.isEmpty()) {
                    insertLookupValues(lookup.getId(), inserts, conn);
                }
                if( !deletes.isEmpty()) {
                    deleteLookupValues(lookup.getId(), deletes, conn);
                }
                conn.commit();
            } catch( SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.close();
            }
        }catch (SQLException e) {
            String message = String.format("Failed to set the lookup values for name %s", name);
            log.error( message, e);
            throw new UserMetadataServiceException( message, e);
        }
        return lookup;
    }

    private void insertLookupValues(Long id, Set<String> inserts, Connection conn) throws SQLException {
        QueryRunner runner = new QueryRunner();
        Object[][] params = new Object[inserts.size()][];
        Iterator<String> iter = inserts.iterator();
        int index = 0;
        while( iter.hasNext()){
           params[index++] = ImmutableList.of( id, iter.next()).toArray();
        }
        runner.batch( conn, SQL_INSERT_LOOKUP_VALUES, params);
    }

    private void deleteLookupValues(Long id, Set<String> deletes, Connection conn) throws SQLException {
        new QueryRunner().update( conn, String.format(SQL_DELETE_LOOKUP_VALUES, "'" + Joiner.on("','").skipNulls().join(deletes) + "'"), id);
    }

    private Lookup findOrCreateLookupByName(String name, Connection conn) throws SQLException {
        Lookup lookup = get(name);
        QueryRunner runner = new QueryRunner();
        if( lookup == null){
            Object[] params = { name, STRING_TYPE, config.getLookupServiceUserAdmin(),
                    config.getLookupServiceUserAdmin() };
            Long lookupId = runner.insert( conn, SQL_INSERT_LOOKUP, new ScalarHandler<>(1), params);
            lookup = new Lookup();
            lookup.setName( name);
            lookup.setId(lookupId);
        }
        return lookup;
    }

    /**
     * Saves the lookup value
     * @param name lookup name
     * @param values multiple values
     * @return
     */
    public Lookup addValues(String name, Set<String> values) {
        Lookup lookup = null;
        try{
            Connection conn = getDataSource().getConnection();
            try {
                lookup = findOrCreateLookupByName( name, conn);
                Set<String> inserts = Sets.newHashSet();
                Set<String> lookupValues = lookup.getValues();
                if( lookupValues == null || lookupValues.isEmpty()){
                    inserts = values;
                    lookup.setValues( values);
                } else {
                    inserts = Sets.difference(values, lookupValues);
                }
                if( !inserts.isEmpty()) {
                    insertLookupValues(lookup.getId(), inserts, conn);
                }
                conn.commit();
            } catch( SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.close();
            }
        }catch (SQLException e) {
            String message = String.format("Failed to set the lookup values for name %s", name);
            log.error( message, e);
            throw new UserMetadataServiceException( message, e);
        }
        return lookup;
    }

    /**
     * Saves the lookup value
     * @param name lookup name
     * @param value lookup value
     * @return
     */
    public Lookup setValue(String name, String value) {
        return setValues(name, Sets.newHashSet(value));
    }
}
