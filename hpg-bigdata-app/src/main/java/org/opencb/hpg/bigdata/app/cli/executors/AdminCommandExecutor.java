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

package org.opencb.hpg.bigdata.app.cli.executors;

import org.opencb.hpg.bigdata.app.cli.CommandExecutor;
import org.opencb.hpg.bigdata.app.cli.options.AdminCommandOptions;
import org.opencb.hpg.bigdata.app.rest.RestServer;

/**
 * Created by imedina on 25/06/15.
 */
public class AdminCommandExecutor extends CommandExecutor {

    private AdminCommandOptions adminCommandOptions;

    public AdminCommandExecutor(AdminCommandOptions adminCommandOptions) {
        super(adminCommandOptions.commonCommandOptions);
        this.adminCommandOptions = adminCommandOptions;
    }


    @Override
    public void execute() throws Exception {
        String subCommandString = getParsedSubCommand(adminCommandOptions.jCommander);
        init(adminCommandOptions.commonCommandOptions.logLevel,
                adminCommandOptions.commonCommandOptions.verbose,
                adminCommandOptions.commonCommandOptions.conf);
        switch (subCommandString) {
            case "server":
                startRestServer();
                break;
            default:
                logger.error("Administration subcommand '" + subCommandString + "' not valid");
                break;
        }
    }

    private void startRestServer() {
        int port = adminCommandOptions.serverAdminCommandOptions.port;

        RestServer restServer = new RestServer(port);
        try {
            restServer.start();
            restServer.blockUntilShutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
