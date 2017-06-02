/*
 * Copyright 2015 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.hpg.bigdata.app;

import com.beust.jcommander.ParameterException;
import org.opencb.hpg.bigdata.app.cli.CommandExecutor;
import org.opencb.hpg.bigdata.app.cli.hadoop.*;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by imedina on 15/03/15.
 */
public class BigDataMain {

    @Deprecated
    public static void main(String[] args) {

        CliOptionsParser cliOptionsParser = new CliOptionsParser();

        if (args == null || args.length == 0) {
            cliOptionsParser.printUsage();
        }

        try {
            cliOptionsParser.parse(args);
        } catch (ParameterException e) {
            System.out.println(e.getMessage());
            cliOptionsParser.printUsage();
            System.exit(-1);
        }

        String parsedCommand = cliOptionsParser.getCommand();
        if (parsedCommand == null || parsedCommand.isEmpty()) {
            if (cliOptionsParser.getGeneralOptions().help) {
                cliOptionsParser.printUsage();
                System.exit(0);
            }
            if (cliOptionsParser.getGeneralOptions().version) {
                printVersion();
            }
        } else {    // correct command exist
            CommandExecutor commandExecutor = null;
            // Check if any command or subcommand -h options are present
            if (cliOptionsParser.getCommandOptions().help || cliOptionsParser.getCommonCommandOptions().help) {
                cliOptionsParser.printUsage();
            } else {
                // get the subcommand and printUsage if empty
                String parsedSubCommand = cliOptionsParser.getSubCommand();
                if (parsedSubCommand == null || parsedSubCommand.isEmpty()) {
                    cliOptionsParser.printUsage();
                } else {
                    switch (parsedCommand) {
                        /*
                        case "sequence":
                            commandExecutor = new SequenceCommandExecutor(cliOptionsParser.getSequenceCommandOptions());
                            break;
                        case "alignment":
                            commandExecutor = new AlignmentCommandExecutor(cliOptionsParser.getAlignmentCommandOptions());
                            break;
                        case "variant":
                            commandExecutor = new VariantCommandExecutor(cliOptionsParser.getVariantCommandOptions());
                            break;
                        case "feature":
                            System.out.printf("Not yet implemented: not valid command: '" + parsedCommand + "'");
                            break;
                            */
                        default:
                            System.out.printf("ERROR: not valid command: '" + parsedCommand + "'");
                            cliOptionsParser.printUsage();
                            break;
                    }
                }
            }

            if (commandExecutor != null) {
                try {
                    commandExecutor.execute();
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(1);
                }
            }
        }
    }

    public static void printVersion() {
        Properties properties = new Properties();
        try {
            properties.load(BigDataMain.class.getClassLoader().getResourceAsStream("conf.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        String version = properties.getProperty("HPG.BIGDATA.VERSION", "<unkwnown>");
        System.out.println("HPG BigData v" + version);
        System.out.println("");
        System.out.println("Read more on https://github.com/opencb/hpg-bigdata");
    }
}
