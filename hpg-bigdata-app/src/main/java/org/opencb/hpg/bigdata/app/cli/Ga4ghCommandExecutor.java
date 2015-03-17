package org.opencb.hpg.bigdata.app.cli;

/**
 * Created by imedina on 16/03/15.
 */
public class Ga4ghCommandExecutor extends CommandExecutor {

    private CliOptionsParser.Ga4ghCommandOptions ga4ghCommandOptions;

    public Ga4ghCommandExecutor(CliOptionsParser.Ga4ghCommandOptions ga4ghCommandOptions) {
        super(ga4ghCommandOptions.commonOptions.logLevel, ga4ghCommandOptions.commonOptions.verbose,
                ga4ghCommandOptions.commonOptions.conf);

        this.ga4ghCommandOptions = ga4ghCommandOptions;
    }


    /**
     * Parse specific 'ga4gh' command options
     */
    public void execute() {
        logger.info("Executing {} CLI options", "ga4gh");

        logger.debug("Input file: {}", ga4ghCommandOptions.input);

    }

}
