
package com.cds.tools.etl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.sql.Connection;
import java.sql.SQLException;


public class OracleConnection implements DbConnection{
    private static final Logger LOG = LoggerFactory.getLogger(OracleConnection.class);

    String url;
    String user;
    String passwd;

    OracleConnection(String url, String user, String passwd) {
        this.url = url;
        this.user = user;
        this.passwd = passwd;
        initDataSource();
    }

    private void initDataSource() {
        dataSource.setDriverClassName("oracle.jdbc.driver.OracleDriver");
        dataSource.setUrl(url);
        dataSource.setPassword(passwd);
        dataSource.setUsername(user);
        LOG.warn("The url is:" + url + " userName is:" + user + " password is:" + passwd);
    }
     public DriverManagerDataSource getDataSource() {
         return dataSource;
    }

    @Override public void closeConnection() {
        try {
            Connection connection = dataSource.getConnection();
            if (null != connection) {
                connection.close();
                LOG.info("the connection is closed!");
            }
        } catch (SQLException e) {
            LOG.error("get connection failed!");
        }
    }

}
