package org.opencb.hpg.bigdata.app.cli;

import com.beust.jcommander.*;

/**
 * Created by imedina on 03/02/15.
 */
public class CliOptionsParser {

    private final JCommander jcommander;

    private final GeneralOptions generalOptions;
    private final CommonCommandOptions commonCommandOptions;

    private FastqCommandOptions fastqCommandOptions;
    private BamCommandOptions bamCommandOptions;
    private Ga4ghCommandOptions ga4ghCommandOptions;
    private AlignCommandOptions alignCommandOptions;

    public CliOptionsParser() {
        generalOptions = new GeneralOptions();

        jcommander = new JCommander(generalOptions);
        jcommander.setProgramName("hpg-bigdata.sh");

        commonCommandOptions = new CommonCommandOptions();

        fastqCommandOptions = new FastqCommandOptions();
        bamCommandOptions = new BamCommandOptions();
        ga4ghCommandOptions = new Ga4ghCommandOptions();
        alignCommandOptions = new AlignCommandOptions();

        jcommander.addCommand("fastq", fastqCommandOptions);
        jcommander.addCommand("bam", bamCommandOptions);
        jcommander.addCommand("ga4gh", ga4ghCommandOptions);
        jcommander.addCommand("align", alignCommandOptions);

    }

    public void parse(String[] args) throws ParameterException {
        jcommander.parse(args);
    }

    public String getCommand() {
        return (jcommander.getParsedCommand() != null) ? jcommander.getParsedCommand(): "";
    }

    public void printUsage(){
        if(getCommand().isEmpty()) {
            jcommander.usage();
        } else {
            jcommander.usage(getCommand());
        }
    }



    public class GeneralOptions {

        @Parameter(names = {"-h", "--help"}, help = true)
        public boolean help;
        @Parameter(names = {"--version"})
        public boolean version;

    }

    public class CommonCommandOptions {

        @Parameter(names = {"-h", "--help"}, help = true)
        public boolean help;
        @Parameter(names = {"-L", "--log-level"}, description = "This parameter set the level of the logging", required = false, arity = 1)
        public String logLevel = "info";

        @Parameter(names = {"-v", "--verbose"}, description = "This parameter set the level of the logging", required = false, arity = 1)
        public boolean verbose;

        @Parameter(names = {"-C", "--conf"}, description = "This parameter set the level of the logging", required = false, arity = 1)
        public String conf;

    }


    @Parameters(commandNames = {"fastq"}, commandDescription = "Description")
    public class FastqCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;


        @Parameter(names = {"-i", "--input"}, description = "", required = true, arity = 1)
        public String input = null;

        @Parameter(names = {"-o", "--output"}, description = "", required = false, arity = 1)
        public String output = null;

        @Parameter(names = {"-s", "--stats"}, description = "", required = false)
        public boolean stats = false;

        @Parameter(names = {"-f", "--filter"}, description = "", required = false, arity = 1)
        public String filter = null;

        @Parameter(names = {"-k", "--kmers"}, description = "", required = false, arity = 1)
        public Integer kmers = 0;

    }


    @Parameters(commandNames = {"bam"}, commandDescription = "Description")
    public class BamCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;


        @Parameter(names = {"-i", "--input"}, description = "", required = true, arity = 1)
        public String input = null;

        @Parameter(names = {"-o", "--output"}, description = "", required = false, arity = 1)
        public String output = null;

        @Parameter(names = {"--stats"}, description = "", required = false)
        public boolean stats = false;

        @Parameter(names = {"--filter"}, description = "", required = false, arity = 1)
        public String filter = null;

        @Parameter(names = {"--index"}, description = "", required = false)
        public boolean index = false;

        @Parameter(names = {"--convert"}, description = "Accepted values: sam2bam, sam2cram, bam2fastq", required = false)
        public boolean convert = false;

        @Parameter(names = {"--to-fastq"}, description = "", required = false)
        public boolean toFastq = false;

    }


    @Parameters(commandNames = {"ga4gh"}, commandDescription = "Description")
    public class Ga4ghCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"}, description = "", required = true, arity = 1)
        public String input = null;

        @Parameter(names = {"-o", "--output"}, description = "", required = false, arity = 1)
        public String output = null;

        @Parameter(names = {"-c", "--conversion"}, description = "Accepted values: fastq2ga, ga2fastq, sam2ga, ga2sam, bam2ga, ga2bam", required = true, arity = 1)
        public String conversion;

        @Parameter(names = {"-x", "--compression"}, description = "Accepted values: snappy, deflate, bzip2, xz. Default: snappy", required = false, arity = 1)
        public String compression;

        @Parameter(names = {"-p", "--parquet"}, description = "Save data in ga4gh using the parquet format (for Hadoop only)", required = false)
        public boolean toParquet = false;
    }


    @Parameters(commandNames = {"align"}, commandDescription = "Description")
    public class AlignCommandOptions {


        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;


        @Parameter(names = {"-i", "--input"}, description = "", required = true, arity = 1)
        public String input = null;

        @Parameter(names = {"-o", "--output"}, description = "", required = false, arity = 1)
        public String output = null;

        @Parameter(names = {"--index-file"}, description = "", required = false)
        public String referenceGenomeFile;


    }


    public GeneralOptions getGeneralOptions() {
        return generalOptions;
    }

    public FastqCommandOptions getFastqCommandOptions() {
        return fastqCommandOptions;
    }

    public BamCommandOptions getBamCommandOptions() {
        return bamCommandOptions;
    }

    public Ga4ghCommandOptions getGa4ghCommandOptions() {
        return ga4ghCommandOptions;
    }

    public AlignCommandOptions getAlignCommandOptions() {
        return alignCommandOptions;
    }

}
