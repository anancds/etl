
package com.cds.tools.etl;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;

public class ParseArgument {

    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(ParseArgument.class);

    public static boolean parse(String[] args) {

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;
        Options options = new Options();
        options.addOption("help", "print ETL tools help message");

        Option databaseName =
            Option.builder("d").argName("databaseName").hasArg().desc("the name of database")
                .build();
        options.addOption(databaseName);

        Option tableName = Option.builder("t").argName("tableName").hasArg()
            .desc("the table name from which the ETL tools can get data").build();
        options.addOption(tableName);

        Option machinesNums = Option.builder("N").argName("machineNums").hasArg()
            .desc("the numbers of cluster machines").build();
        options.addOption(machinesNums);

        Option userName =
            Option.builder("U").argName("userName").hasArg().desc("the Oracle database's userName")
                .build();
        options.addOption(userName);

        Option instanceName =
            Option.builder("i").argName("instanceName").hasArg().desc("the Oracle instance's name")
                .build();
        options.addOption(instanceName);

        Option passWord =
            Option.builder("p").argName("password").hasArg().desc("the Oracle database's password")
                .build();
        options.addOption(passWord);

        Option url = Option.builder("u").argName("url").hasArg().desc("the oracle's url").build();
        options.addOption(url);

        Option num = Option.builder("n").argName("numOfMachine").hasArg()
            .desc("the number of the machine of the cluster begin from 0, eg. 0 1 2 3 4 5 ")
            .build();
        options.addOption(num);

        Option totalNum = Option.builder("T").argName("totalNum").hasArg()
            .desc("the total number of the database").build();
        options.addOption(totalNum);


        try {
            cmd = parser.parse(options, args);

            if (args.length == 0 || args[0].equals("-help")) {

                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("ETL tools [options]", options);
                return false;
            }

            if (cmd.hasOption("n")) {
                ETLConsts.INITRECORD_ID =
                    ETLConsts.INITRECORD_ID * (Integer.valueOf(cmd.getOptionValue("n")) + 1);
            }

            if (cmd.hasOption("t")) {
                ETLConsts.TABLENAME = cmd.getOptionValue("t");
            }

            if (cmd.hasOption("T")) {
                ETLConsts.TOTALNUM = Long.valueOf(cmd.getOptionValue("T"));
                ETLConsts.DATAPERNODE = ETLConsts.TOTALNUM / Integer.valueOf(ETLConsts.MACHINENUM);
            }

            if (cmd.hasOption("p")) {
                ETLConsts.PASSWORD = cmd.getOptionValue("p");
            }

            if (cmd.hasOption("U")) {
                ETLConsts.USERNAME = cmd.getOptionValue("U");
            }

            if (cmd.hasOption("u")) {
                ETLConsts.URL = cmd.getOptionValue("u");
            }


            if (cmd.hasOption("i")) {
                ETLConsts.INSTANCENAME = cmd.getOptionValue("i");
            }

            if (cmd.hasOption("d")) {
                ETLConsts.DATABASENAME = cmd.getOptionValue("d");
            }

            if (cmd.hasOption("n")) {
                ETLConsts.NUMOFMACHINE = cmd.getOptionValue("n");
            }

            if (cmd.hasOption("N")) {
                ETLConsts.MACHINENUM = cmd.getOptionValue("N");
            }



        } catch (ParseException e) {
            LOG.error("Parse argument failed!", e);
        }

        return true;
    }
}
