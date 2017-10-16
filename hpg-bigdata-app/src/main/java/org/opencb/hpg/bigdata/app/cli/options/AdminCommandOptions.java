package org.opencb.hpg.bigdata.app.cli.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.hpg.bigdata.app.cli.LocalCliOptionsParser;

/**
 * Created by jtarraga on 01/06/17.
 */
@Parameters(commandNames = {"admin"}, commandDescription = "Implements different admin tasks")
public class AdminCommandOptions {

    /*
     * Admin CLI options
     */
    public ServerAdminCommandOptions serverAdminCommandOptions;

    public LocalCliOptionsParser.CommonCommandOptions commonCommandOptions;
    public JCommander jCommander;

    public AdminCommandOptions(LocalCliOptionsParser.CommonCommandOptions commonCommandOptions,
                               JCommander jCommander) {
        this.commonCommandOptions = commonCommandOptions;
        this.jCommander = jCommander;

        this.serverAdminCommandOptions = new ServerAdminCommandOptions();
    }

    @Parameters(commandNames = {"server"}, commandDescription = "Converts FastQ files to different big data formats"
            + " such as Avro")
    public class ServerAdminCommandOptions {

        @ParametersDelegate
        public LocalCliOptionsParser.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-p", "--port"}, description = "Input file name, usually a gVCF/VCF but it can be an Avro"
                + " file when converting to Parquet.", required = true, arity = 1)
        public int port = 1042;

    }

}
