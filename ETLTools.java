
package com.cds.tools.etl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ETLTools {

    private static final Logger LOG = LoggerFactory.getLogger(ETLTools.class);

    public static void main(String[] args) {
        LOG.info("begin to get oracle data!");
        boolean isSuccess = ParseArgument.parse(args);
        if (!isSuccess) {
            LOG.error("the argument is invalid!");
            System.exit(0);
        }
        ETLRun run = new ETLRun();
        Thread thread = new Thread(run);
        thread.start();
    }
}
