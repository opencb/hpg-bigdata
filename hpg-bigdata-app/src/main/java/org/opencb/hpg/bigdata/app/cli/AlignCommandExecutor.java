package org.opencb.hpg.bigdata.app.cli;

/**
 * Created by imedina on 16/03/15.
 */
public class AlignCommandExecutor extends CommandExecutor {

    private CliOptionsParser.AlignCommandOptions alignCommandOptions;

    public AlignCommandExecutor(CliOptionsParser.AlignCommandOptions alignCommandOptions) {
        super(alignCommandOptions.commonOptions.logLevel, alignCommandOptions.commonOptions.verbose,
                alignCommandOptions.commonOptions.conf);

        this.alignCommandOptions = alignCommandOptions;
    }


    /**
     * Parse specific 'align' command options
     */
    public void parse() {
        logger.info("Executing {} CLI options", "align");

        logger.debug("Input file: {}", alignCommandOptions.input);
    }

}
