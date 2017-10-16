package org.opencb.hpg.bigdata.app.cli.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.hpg.bigdata.app.cli.LocalCliOptionsParser;

/**
 * Created by jtarraga on 01/06/17.
 */
@Parameters(commandNames = {"sequence"}, commandDescription = "Implements different tools for working with Fastq files")
public class SequenceCommandOptions {

    public ConvertSequenceCommandOptions convertSequenceCommandOptions;
    public StatsSequenceCommandOptions statsSequenceCommandOptions;

    public LocalCliOptionsParser.CommonCommandOptions commonCommandOptions;
    public JCommander jCommander;

    public SequenceCommandOptions(LocalCliOptionsParser.CommonCommandOptions commonCommandOptions,
                                  JCommander jCommander) {
        this.commonCommandOptions = commonCommandOptions;
        this.jCommander = jCommander;

        this.convertSequenceCommandOptions = new ConvertSequenceCommandOptions();
        this.statsSequenceCommandOptions = new StatsSequenceCommandOptions();
    }

    @Parameters(commandNames = {"convert"}, commandDescription = "Converts FastQ files to different big data formats"
            + " such as Avro")
    public class ConvertSequenceCommandOptions {

        @ParametersDelegate
        public LocalCliOptionsParser.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"}, description = "Local input file in FastQ format", required = true,
                arity = 1)
        public String input = null;

        @Parameter(names = {"-o", "--output"}, description = "Local output file to store the FastQ sequences according"
                + " to the GA4GH/Avro model", required = true, arity = 1)
        public String output = null;

        @Parameter(names = {"-x", "--compression"}, description = "Accepted values: snappy, deflate, bzip2, xz, null."
                + " Default: snappy", arity = 1)
        public String compression = "snappy";

        //@Parameter(names = {"--to-avro"}, description = "", required = false)
        //public boolean toAvro = true;

        //@Parameter(names = {"--to-fastq"}, description = "", required = false)
        //public boolean toFastq;
    }

    @Parameters(commandNames = {"stats"}, commandDescription = "Calculates different stats from sequencing data")
    public class StatsSequenceCommandOptions {

        @ParametersDelegate
        public LocalCliOptionsParser.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"}, description = "Local input file containing the FastQ sequences stored"
                + " according to the GA4GH/Avro model)", required = true, arity = 1)
        public String input = null;

        @Parameter(names = {"-o", "--output"}, description = "Local output directory to save stats results in JSON"
                + " format ", required = true, arity = 1)
        public String output = null;

        //@Parameter(names = {"-f", "--filter"}, description = "", required = false, arity = 1)
        //public String filter = null;

        @Parameter(names = {"-k", "--kmers"}, description = "Compute k-mers (according to the indicated length)", arity = 1)
        public Integer kmers = 0;
    }

}
