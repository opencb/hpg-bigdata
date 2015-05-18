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
    private ConvertCommandOptions convertCommandOptions;
    private AlignCommandOptions alignCommandOptions;

    public CliOptionsParser(boolean hadoop) {
        generalOptions = new GeneralOptions();

        jcommander = new JCommander(generalOptions);

        commonCommandOptions = new CommonCommandOptions();

        convertCommandOptions = new ConvertCommandOptions();
        jcommander.addCommand(convertCommandOptions);
        if (hadoop) {
            jcommander.setProgramName("bin/hpg-bigdata.sh");
            fastqCommandOptions = new FastqCommandOptions();
            bamCommandOptions = new BamCommandOptions();
            alignCommandOptions = new AlignCommandOptions();

            jcommander.addCommand(fastqCommandOptions);
            jcommander.addCommand(bamCommandOptions);
            jcommander.addCommand(alignCommandOptions);
        } else {    //local
            jcommander.setProgramName("bin/hpg-bigdata-local.sh");
        }
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


        @Parameter(names = {"-i", "--input"}, description = "HDFS input file (the FastQ file must be stored in GA4GH/Avro model)", required = true, arity = 1)
        public String input = null;

        @Parameter(names = {"-o", "--output"}, description = "Local output directory to save results, summary, images...", required = false, arity = 1)
        public String output = null;

        @Parameter(names = {"-s", "--stats"}, description = "Run statistics", required = false)
        public boolean stats = false;

        @Parameter(names = {"-f", "--filter"}, description = "", required = false, arity = 1)
        public String filter = null;

        @Parameter(names = {"-k", "--kmers"}, description = "Compute k-mers (according to the indicated length)", required = false, arity = 1)
        public Integer kmers = 0;

    }


    @Parameters(commandNames = {"bam"}, commandDescription = "Description")
    public class BamCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;


        @Parameter(names = {"-i", "--input"}, description = "HDFS input file (the BAM/SAM file must be stored in GA4GH/Avro model)", required = true, arity = 1)
        public String input = null;

        @Parameter(names = {"-o", "--output"}, description = "Local output directory to save results, summary, images...", required = false, arity = 1)
        public String output = null;

        @Parameter(names = {"--command"}, description = "Accepted values: stats, sort, depth, to-parquet", required = false, arity = 1)
        public String command = null;

        @Parameter(names = {"--filter"}, description = "", required = false, arity = 1)
        public String filter = null;

        @Parameter(names = {"--index"}, description = "", required = false)
        public boolean index = false;

        @Parameter(names = {"--convert"}, description = "Accepted values: sam2bam, sam2cram, bam2fastq", required = false)
        public boolean convert = false;

        @Parameter(names = {"--to-fastq"}, description = "", required = false)
        public boolean toFastq = false;

        @Parameter(names = {"-x", "--compression"}, description = "For the command: 'to-parquet'. Accepted values: snappy, deflate, bzip2, xz. Default: snappy", required = false, arity = 1)
        public String compression = "snappy";
        
    }


    public static class ConvertionConverter implements IStringConverter<ConvertCommandExecutor.Conversion> {
        @Override
        public ConvertCommandExecutor.Conversion convert(String s) {
            ConvertCommandExecutor.Conversion conversion = ConvertCommandExecutor.Conversion.fromName(s);
            if (conversion == null) {
                String message = "Value " + s + " is not a valid conversion. Accepted values: \n" +
                        ConvertCommandExecutor.Conversion.getValidConversionString();

                throw new ParameterException(message);
            }
            return conversion;
        }
    }

    @Parameters(commandNames = {"convert"}, commandDescription = "Convert between different formats")
    public class ConvertCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"}, description = "", required = true, arity = 1)
        public String input = null;

        @Parameter(names = {"-o", "--output"}, description = "", required = false, arity = 1)
        public String output = null;

        @Parameter(names = {"-c", "--conversion"}, description = "Accepted values: fastq2ga, ga2fastq, sam2ga, ga2sam, bam2ga, ga2bam", required = true, arity = 1, converter = ConvertionConverter.class)
        public ConvertCommandExecutor.Conversion conversion;

        @Parameter(names = {"-x", "--compression"}, description = "Accepted values: snappy, deflate, bzip2, xz. Default: snappy", required = false, arity = 1)
        public String compression;

        @Parameter(names = {"-p", "--parquet"}, description = "Save data in ga4gh using the parquet format (for Hadoop only)", required = false)
        public boolean toParquet = false;
    }

    @Parameters(commandNames = {"load-hbase"}, commandDescription = "Load")
    public class LoadHBaseCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"}, description = "Avro input files", required = true, arity = 1)
        public String input = null;

        @Parameter(names = {"-C", "--credentials"}, description = "", required = false, arity = 1)
        public String credentials;

    }

    @Parameters(commandNames = {"load-parquet"}, commandDescription = "Load")
    public class LoadHiveCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"}, description = "Avro or Parquet input files", required = true, arity = 1)
        public String input = null;

        @Parameter(names = {"-o", "--output"}, description = "", required = false, arity = 1)
        public String output = null;

        @Parameter(names = {"-C", "--credentials"}, description = "", required = false, arity = 1)
        public String credentials;
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

    public ConvertCommandOptions getConvertCommandOptions() {
        return convertCommandOptions;
    }

    public AlignCommandOptions getAlignCommandOptions() {
        return alignCommandOptions;
    }

}
