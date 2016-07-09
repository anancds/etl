
package com.cds.tools.etl;

public class ConnectionFactory {

    public DbConnection getDBConnection(String dbType, String url, String userName, String passwd) {
        if (dbType == null)
            return null;
        if ("oracle".equalsIgnoreCase(dbType))
            return new OracleConnection(url, userName, passwd);
        return null;
    }
}
