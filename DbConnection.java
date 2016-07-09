
package com.cds.tools.etl;


import org.springframework.jdbc.datasource.DriverManagerDataSource;

public interface DbConnection {

    DriverManagerDataSource dataSource = new DriverManagerDataSource();

    DriverManagerDataSource getDataSource();

    void closeConnection();
}
