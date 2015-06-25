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

package org.opencb.hpg.bigdata.app.cli.local;

import com.beust.jcommander.*;
import org.opencb.hpg.bigdata.app.cli.hadoop.ConvertCommandExecutor;

import java.util.List;

/**
 * Created by imedina on 16/06/15.
 */
public class CliOptionsParser {

    private final JCommander jcommander;

    private final GeneralOptions generalOptions;


    private final CommonCommandOptions commonCommandOptions;

    private SequenceCommandOptions sequenceCommandOptions;
    private ConvertCommandOptions convertCommandOptions;
    private AlignmentCommandOptions alignmentCommandOptions;
    private AlignmentConvertCommandOptions alignmentConvertCommandOptions;

    public CliOptionsParser() {

        generalOptions = new GeneralOptions();

        jcommander = new JCommander(generalOptions);
        jcommander.setProgramName("hpg-bigdata-local.sh");

        commonCommandOptions = new CommonCommandOptions();

        alignmentCommandOptions= new AlignmentCommandOptions();
        alignmentConvertCommandOptions = new AlignmentConvertCommandOptions();

        convertCommandOptions = new ConvertCommandOptions();
        jcommander.addCommand("a", alignmentCommandOptions);
        jcommander.addCommand("alignment", alignmentCommandOptions);

        jcommander.getCommands().get("alignment").addCommand("convert", alignmentConvertCommandOptions);


    }

    public void parse(String[] args) throws ParameterException {
        jcommander.parse(args);
    }

    public String getCommand() {
        return (jcommander.getParsedCommand() != null) ? jcommander.getParsedCommand(): "";
    }

    public boolean isHelp() {
        String parsedCommand = jcommander.getParsedCommand();
        if (parsedCommand != null) {
            JCommander jCommander = jcommander.getCommands().get(parsedCommand);
            List<Object> objects = jCommander.getObjects();
            if (!objects.isEmpty() && objects.get(0) instanceof CommonCommandOptions) {
                return ((CommonCommandOptions) objects.get(0)).help;
            }
        }
        return getCommonCommandOptions().help;
    }

    public void printUsage(){
        if(getCommand().isEmpty()) {
            jcommander.usage();
        } else {
            jcommander.usage(getCommand());
        }
    }


    /**
     * This class contains all those parameters that are intended to work without any 'command'
     */
    public class GeneralOptions {

        @Parameter(names = {"-h", "--help"}, help = true)
        public boolean help;

        @Parameter(names = {"--version"})
        public boolean version;

    }

    /**
     * This class contains all those parameters available for all 'commands'
     */
    public class CommonCommandOptions {

        @Parameter(names = {"-h", "--help"}, help = true)
        public boolean help;

        @Parameter(names = {"-L", "--log-level"}, description = "This parameter set the level of the logging", required = false, arity = 1)
        public String logLevel = "info";

        @Deprecated
        @Parameter(names = {"-v", "--verbose"}, description = "This parameter set the level of the logging", required = false, arity = 1)
        public boolean verbose;

        @Parameter(names = {"-C", "--conf"}, description = "This ia a reserved parameter for configuration file", required = false, arity = 1)
        public String configurationFile;

    }


    @Parameters(commandNames = {"sequence"}, commandDescription = "Description")
    public class SequenceCommandOptions extends CommonCommandOptions {

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

    @Parameters(commandNames = {"alignment"}, commandDescription = "Description")
    public class AlignmentCommandOptions  extends CommonCommandOptions {

        @Parameter(names = {"-i", "--input"}, description = "", required = true, arity = 1)
        public String input = null;

        @Parameter(names = {"-o", "--output"}, description = "", required = false, arity = 1)
        public String output = null;

        @Parameter(names = {"--index-file"}, description = "", required = false)
        public String referenceGenomeFile;

    }

    @Parameters(commandNames = {"bam"}, commandDescription = "Description")
    public class AlignmentConvertCommandOptions {


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

    @Parameters(commandNames = {"convert"}, commandDescription = "Convert between different formats")
    public class ConvertCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"}, description = "", required = true, arity = 1)
        public String input = null;

        @Parameter(names = {"-o", "--output"}, description = "", required = false, arity = 1)
        public String output = null;

        @Parameter(names = {"-c", "--conversion"}, description = "Accepted values: fastq2avro, avro2fastq, sam2avro, avro2sam, bam2avro, avro2bam, vcf2avro", required = true, arity = 1)
        public ConvertCommandExecutor.Conversion conversion;

        @Parameter(names = {"-x", "--compression"}, description = "Accepted values: snappy, deflate, bzip2, xz. [snappy]", required = false, arity = 1)
        public String compression = "snappy";

        @Parameter(names = {"-p", "--to-avro"}, description = "Serialize data to GA4GH Avro format [true]", required = false)
        public boolean toAvro = true;

        @Parameter(names = {"-p", "--to-avro"}, description = "Serialize data from  GA4GH Avro format [true]", required = false)
        public boolean fromAvro = false;
        @Parameter(names = {"-p", "--to-parquet"}, description = "Save data in ga4gh using the parquet format (for Hadoop only)", required = false)
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



    public GeneralOptions getGeneralOptions() {
        return generalOptions;
    }

    public CommonCommandOptions getCommonCommandOptions() {
        return commonCommandOptions;
    }

    public SequenceCommandOptions getSequenceCommandOptions() {
        return sequenceCommandOptions;
    }

    public AlignmentCommandOptions getAlignmentCommandOptions() {
        return alignmentCommandOptions;
    }

    public AlignmentConvertCommandOptions getAlignmentConvertCommandOptions() {
        return alignmentConvertCommandOptions;
    }

    public ConvertCommandOptions getConvertCommandOptions() {
        return convertCommandOptions;
    }
}
