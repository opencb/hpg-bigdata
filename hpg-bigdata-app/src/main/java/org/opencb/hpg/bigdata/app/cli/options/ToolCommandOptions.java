package org.opencb.hpg.bigdata.app.cli.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.hpg.bigdata.app.cli.LocalCliOptionsParser;

import java.util.List;

/**
 * Created by jtarraga on 02/06/17.
 */
@Parameters(commandNames = {"tools"}, commandDescription = "Run external tools and analysis")
public class ToolCommandOptions {

    public JCommander jCommander;

    @ParametersDelegate
    public LocalCliOptionsParser.CommonCommandOptions commonCommandOptions;

    @Parameter(names = {"--id"}, description = "Tool and execution id separated by ':'. When there is only one execution, only the "
            + "tool id will be needed. Example: samtools:view.", required = true, arity = 1)
    public String tool;

    @Parameter(names = {"--path"}, description = "Path containing all the tools.", required = true, arity = 1)
    public String path;

    @Parameter(names = {"--params"}, description = "List of space-separated key=value parameters necessary to run the tool.",
            required = true, variableArity = true)
    public List<String> params;

    public ToolCommandOptions(LocalCliOptionsParser.CommonCommandOptions commonCommandOptions,
                                  JCommander jCommander) {
        this.commonCommandOptions = commonCommandOptions;
        this.jCommander = jCommander;
    }

}
