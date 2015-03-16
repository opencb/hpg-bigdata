package org.opencb.hpg.bigdata.app.cli;

/**
 * Created by imedina on 03/02/15.
 */
public class FastqCommandExecutor extends CommandExecutor {

    private CliOptionsParser.FastqCommandOptions fastqCommandOptions;

    public FastqCommandExecutor(CliOptionsParser.FastqCommandOptions fastqCommandOptions) {
        super(fastqCommandOptions.commonOptions.logLevel, fastqCommandOptions.commonOptions.verbose,
                fastqCommandOptions.commonOptions.conf);

        this.fastqCommandOptions = fastqCommandOptions;
    }


    /**
     * Parse specific 'fastq' command options
     */
    public void parse() {
        logger.info("Executing {} CLI options", "Fastq");

        logger.debug("Input file: {}", fastqCommandOptions.intput);
    }

}
