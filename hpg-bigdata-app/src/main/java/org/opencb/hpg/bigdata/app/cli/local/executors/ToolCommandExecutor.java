package org.opencb.hpg.bigdata.app.cli.local.executors;

import org.apache.commons.lang3.StringUtils;
import org.opencb.hpg.bigdata.analysis.tools.ToolManager;
import org.opencb.hpg.bigdata.app.cli.CommandExecutor;
import org.opencb.hpg.bigdata.app.cli.local.options.ToolCommandOptions;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by pfurio on 01/06/17.
 */
public class ToolCommandExecutor extends CommandExecutor {

    private ToolCommandOptions toolCommandOptions;

    public ToolCommandExecutor(ToolCommandOptions toolCommandOptions) {
        super(toolCommandOptions.commonCommandOptions);
        this.toolCommandOptions = toolCommandOptions;
    }

    @Override
    public void execute() throws Exception {
        // Obtain the toolId and executionId
        String[] split = StringUtils.split(toolCommandOptions.tool, ":", 2);
        String toolId = split[0];
        String executionId = split.length > 1 ? split[1] : null;

        // Parse the params into an map
        Map<String, Object> params = new HashMap<>(toolCommandOptions.params.size());
        for (String param : toolCommandOptions.params) {
            split = StringUtils.split(param, "=", 2);
            params.put(split[0], split.length > 1 ? split[1] : null);
        }



        ToolManager toolManager = new ToolManager(Paths.get(toolCommandOptions.path));
        String commandLine = toolManager.createCommandLine(toolId, executionId, params);
        toolManager.runCommandLine(commandLine, Paths.get("").toAbsolutePath());
    }
}
