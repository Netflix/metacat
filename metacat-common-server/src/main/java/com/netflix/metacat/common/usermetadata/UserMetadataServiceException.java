package com.netflix.metacat.common.usermetadata;

import java.sql.SQLException;

/**
 * Created by amajumdar on 3/16/16.
 */
public class UserMetadataServiceException extends RuntimeException {
    public UserMetadataServiceException(String m, Exception e) {
        super(m, e);
    }
}
