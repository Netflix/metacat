package com.netflix.metacat.common.usermetadata;

import com.netflix.metacat.common.model.Lookup;

import java.util.Set;

/**
 * Created by amajumdar on 7/6/15.
 */
public interface LookupService {
    /**
     * Returns the lookup for the given <code>name</code>
     * @param name lookup name
     * @return lookup
     */
    Lookup get(String name);
    /**
     * Returns the value of the lookup name
     * @param name lookup name
     * @return scalar lookup value
     */
    String getValue(String name);
    /**
     * Returns the list of values of the lookup name
     * @param name lookup name
     * @return list of lookup values
     */
    Set<String> getValues(String name);
    /**
     * Returns the list of values of the lookup name
     * @param lookupId lookup id
     * @return list of lookup values
     */
    Set<String> getValues(Long lookupId);
    /**
     * Saves the lookup value
     * @param name lookup name
     * @param values multiple values
     * @return
     */
    Lookup setValues(String name, Set<String> values);
    /**
     * Saves the lookup value
     * @param name lookup name
     * @param values multiple values
     * @return
     */
    Lookup addValues(String name, Set<String> values);
    /**
     * Saves the lookup value
     * @param name lookup name
     * @param value lookup value
     * @return
     */
    Lookup setValue(String name, String value);
}
