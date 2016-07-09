
package com.cds.tools.etl;

import com.uniview.salut.common.kafka.producer.KafKaProducerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.util.List;
import java.util.Map;

public class ETLRun implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ETLRun.class);

    DriverManagerDataSource dataSource;
    DbConnection connection;
    ConnectionFactory factory = new ConnectionFactory();
    String DatabaseUrl;
    JdbcTemplate template;
    KafKaProducerClient kafkaClient;

    long index;
    long beginIndex;
    long endIndex;
    byte[] dataToSend;

    private void init() {

        switch (ETLConsts.DATABASENAME) {
            case "oracle":
                DatabaseUrl =
                    "jdbc:oracle:thin:@" + ETLConsts.URL + ":1521:" + ETLConsts.INSTANCENAME;
                break;
            default:
                break;
        }

        connection = factory
            .getDBConnection(ETLConsts.DATABASENAME, DatabaseUrl, ETLConsts.USERNAME,
                ETLConsts.PASSWORD);

        dataSource = connection.getDataSource();
        template = new JdbcTemplate(dataSource);
        kafkaClient = DBUtils.getKafkaClient(DBUtils.kafkaHost);

        LOG.info("The kafka host is:" + DBUtils.kafkaHost);
        LOG.info("The database name is:" + ETLConsts.DATABASENAME + " The database url is: "
            + DatabaseUrl + " The database userName is: " + ETLConsts.USERNAME

            + " The database Password is: " + ETLConsts.PASSWORD);

    }

    @Override public void run() {
        init();

        long begin = System.currentTimeMillis();
        beginIndex = ETLConsts.DATAPERNODE * Integer.valueOf(ETLConsts.NUMOFMACHINE);
        if (Integer.valueOf(ETLConsts.NUMOFMACHINE) < (Integer.valueOf(ETLConsts.MACHINENUM) - 1)) {
            endIndex = ETLConsts.DATAPERNODE * (Integer.valueOf(ETLConsts.NUMOFMACHINE) + 1);

        } else {
            endIndex = ETLConsts.TOTALNUM + 1;
        }

        LOG.warn("this is the " + ETLConsts.NUMOFMACHINE + " machine!");
        LOG.warn("the begin index is: " + beginIndex + " the end index is: " + endIndex);

        for (index = beginIndex; index <= endIndex - 999; index += 1000) {
            List<Map<String, Object>> datas =
                DBUtils.queryDb(template, ETLConsts.TABLENAME, index, index + 1000);
            for (Map<String, Object> data : datas) {
                if (null != DBUtils.oracleToKafka(data)) {
                    kafkaClient.sendSync(DBUtils.oracleToKafka(data));
                }
            }
        }

        if (index < endIndex) {
            List<Map<String, Object>> datas =
                DBUtils.queryDb(template, ETLConsts.TABLENAME, index, endIndex);
            for (Map<String, Object> data : datas) {
                dataToSend = DBUtils.oracleToKafka(data);
                if (null != dataToSend) {
                    kafkaClient.sendSync(DBUtils.oracleToKafka(data));
                }
            }
        }
        try {
            kafkaClient.destroy();
            connection.closeConnection();
        } catch (Exception e) {
            LOG.error("destroy kafkaClient failed!");
        }

        LOG.info("finished, the time cost is:" + (System.currentTimeMillis() - begin) / (1000 * 60)
            + " minutes!");
    }
}
