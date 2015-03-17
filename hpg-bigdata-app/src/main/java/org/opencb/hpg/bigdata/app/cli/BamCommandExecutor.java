package org.opencb.hpg.bigdata.app.cli;

/**
 * Created by imedina on 15/03/15.
 */
public class BamCommandExecutor extends CommandExecutor {

    private CliOptionsParser.BamCommandOptions bamCommandOptions;

    public BamCommandExecutor(CliOptionsParser.BamCommandOptions bamCommandOptions) {
        super(bamCommandOptions.commonOptions.logLevel, bamCommandOptions.commonOptions.verbose,
                bamCommandOptions.commonOptions.conf);

        this.bamCommandOptions = bamCommandOptions;
    }


    /**
     * Parse specific 'bam' command options
     */
    public void execute() {
        logger.info("Executing {} CLI options", "bam");

        logger.debug("Input file: {}", bamCommandOptions.input);

    }

}
