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

package org.opencb.hpg.bigdata.app.cli.hadoop;

import com.beust.jcommander.*;

import java.util.Map;

/**
 * Created by imedina on 03/02/15.
 */
public class CliOptionsParser {

    private final JCommander jcommander;

    private final GeneralOptions generalOptions;
    private final CommandOptions commandOptions;

    private final CommonCommandOptions commonCommandOptions;

    // NGS Sequence command and subcommmands
    private SequenceCommandOptions sequenceCommandOptions;

    // NGS Alignments command and subcommmands
    private AlignmentCommandOptions alignmentCommandOptions;

    // NGS variant command and subcommmands
    private VariantCommandOptions variantCommandOptions;

    private ConvertCommandOptions convertCommandOptions;
//    private AlignSequenceCommandOptions alignSequenceCommandOptions;
//    private IndexCommandOptions indexCommandOptions;


    public CliOptionsParser() {
        generalOptions = new GeneralOptions();
        jcommander = new JCommander(generalOptions);

        commandOptions = new CommandOptions();
        commonCommandOptions = new CommonCommandOptions();

        sequenceCommandOptions = new SequenceCommandOptions();
        jcommander.addCommand("sequence", sequenceCommandOptions);
        JCommander sequenceSubCommands = jcommander.getCommands().get("sequence");
        sequenceSubCommands.addCommand("convert", sequenceCommandOptions.convertSequenceCommandOptions);
        sequenceSubCommands.addCommand("stats", sequenceCommandOptions.statsSequenceCommandOptions);
        sequenceSubCommands.addCommand("align", sequenceCommandOptions.alignSequenceCommandOptions);

        alignmentCommandOptions = new AlignmentCommandOptions();
        jcommander.addCommand("alignment", sequenceCommandOptions);
        JCommander alignmentSubCommands = jcommander.getCommands().get("alignment");
        alignmentSubCommands.addCommand("convert", alignmentCommandOptions.convertAlignmentCommandOptions);
        alignmentSubCommands.addCommand("stats", alignmentCommandOptions.statsAlignmentCommandOptions);

        variantCommandOptions = new VariantCommandOptions();
        jcommander.addCommand("variant", sequenceCommandOptions);
        JCommander variantSubCommands = jcommander.getCommands().get("variant");
        variantSubCommands.addCommand("convert", variantCommandOptions.convertVariantCommandOptions);
        variantSubCommands.addCommand("load", variantCommandOptions.loadVariantCommandOptions);

//        convertCommandOptions = new ConvertCommandOptions();
    }

    public void parse(String[] args) throws ParameterException {
        jcommander.parse(args);
    }

    public String getCommand() {
        return (jcommander.getParsedCommand() != null) ? jcommander.getParsedCommand(): "";
    }

    public String getSubCommand() {
        String parsedCommand = jcommander.getParsedCommand();
        if (jcommander.getCommands().containsKey(parsedCommand)) {
            String subCommand = jcommander.getCommands().get(parsedCommand).getParsedCommand();
            return subCommand != null ? subCommand: "";
        } else {
            return null;
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
    public class CommandOptions {

        @Parameter(names = {"-h", "--help"}, help = true)
        public boolean help;

        public JCommander getSubCommand() {
            return jcommander.getCommands().get(getCommand()).getCommands().get(getSubCommand());
        }

        public String getParsedSubCommand() {
            String parsedCommand = jcommander.getParsedCommand();
            if (jcommander.getCommands().containsKey(parsedCommand)) {
                String subCommand = jcommander.getCommands().get(parsedCommand).getParsedCommand();
                return subCommand != null ? subCommand: "";
            } else {
                return "";
            }
        }
    }

    /**
     * This class contains all those parameters available for all 'subcommands'
     */
    public class CommonCommandOptions {

        @Parameter(names = {"-h", "--help"}, help = true)
        public boolean help;

        @Parameter(names = {"-L", "--log-level"}, description = "This parameter set the level of the logging", required = false, arity = 1)
        public String logLevel = "info";

        @Deprecated
        @Parameter(names = {"-v", "--verbose"}, description = "This parameter set the level of the logging", required = false, arity = 1)
        public boolean verbose;

        @Parameter(names = {"--conf"}, description = "This ia a reserved parameter for configuration file", required = false, arity = 1)
        public String conf;

    }


    /*
     * Sequence (FASTQ) CLI options
     */

    @Parameters(commandNames = {"sequence"}, commandDescription = "Different tools for working with NGS Fastq files")
    public class SequenceCommandOptions extends CommandOptions {

        ConvertSequenceCommandOptions convertSequenceCommandOptions;
        StatsSequenceCommandOptions statsSequenceCommandOptions;
        AlignSequenceCommandOptions alignSequenceCommandOptions;

        public SequenceCommandOptions() {
            this.convertSequenceCommandOptions = new ConvertSequenceCommandOptions();
            this.statsSequenceCommandOptions = new StatsSequenceCommandOptions();
            this.alignSequenceCommandOptions = new AlignSequenceCommandOptions();
        }
    }

    @Parameters(commandNames = {"convert"}, commandDescription = "Create new user for OpenCGA-Catalog")
    class ConvertSequenceCommandOptions {

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

    @Parameters(commandNames = {"stats"}, commandDescription = "Create new user for OpenCGA-Catalog")
    class StatsSequenceCommandOptions {

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

    @Parameters(commandNames = {"align"}, commandDescription = "Description")
    public class AlignSequenceCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"}, description = "", required = true, arity = 1)
        public String input = null;

        @Parameter(names = {"-o", "--output"}, description = "", required = false, arity = 1)
        public String output = null;

        @Parameter(names = {"--index-file"}, description = "", required = false)
        public String referenceGenomeFile;

    }



    /*
     * Alignment (BAM) CLI options
     */
    @Parameters(commandNames = {"alignment"}, commandDescription = "Different tools for working with NGS SAM/BAM files")
    public class AlignmentCommandOptions extends CommandOptions {

        ConvertAlignmentCommandOptions convertAlignmentCommandOptions;
        StatsAlignmentCommandOptions statsAlignmentCommandOptions;

        public AlignmentCommandOptions() {
            this.convertAlignmentCommandOptions = new ConvertAlignmentCommandOptions();
            this.statsAlignmentCommandOptions = new StatsAlignmentCommandOptions();
        }
    }

    @Parameters(commandNames = {"convert"}, commandDescription = "Create new user for OpenCGA-Catalog")
    class ConvertAlignmentCommandOptions {

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

    @Parameters(commandNames = {"stats"}, commandDescription = "Create new user for OpenCGA-Catalog")
    class StatsAlignmentCommandOptions {

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

    }


    /*
     * Variant (VCF) CLI options
     */
    @Parameters(commandNames = {"variant"}, commandDescription = "Description")
    public class VariantCommandOptions extends CommandOptions {

        ConvertVariantCommandOptions convertVariantCommandOptions;
        LoadVariantCommandOptions loadVariantCommandOptions;

        public VariantCommandOptions() {
            this.convertVariantCommandOptions = new ConvertVariantCommandOptions();
            this.loadVariantCommandOptions = new LoadVariantCommandOptions();
        }
    }

    @Parameters(commandNames = {"convert"}, commandDescription = "Create new user for OpenCGA-Catalog")
    class ConvertVariantCommandOptions {

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

    @Parameters(commandNames = {"load-hbase"}, commandDescription = "Load")
    public class LoadVariantCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"}, description = "Avro input files", required = true, arity = 1)
        public String input = null;

        @Parameter(names = {"-C", "--credentials"}, description = "", required = false, arity = 1)
        public String credentials;

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

    @Parameters(commandNames = {"index"}, commandDescription = "Description")
    public class IndexCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;


        @Parameter(names = {"-i", "--input"}, description = "", required = true, arity = 1)
        public String input = null;

        @Parameter(names = {"-o", "--output"}, description = "", required = false, arity = 1)
        public String output = null;

        @Parameter(names = {"--index-file"}, description = "", required = false)
        public String referenceGenomeFile;

    }


    public void printUsage(){
        if(getCommand().isEmpty()) {
            System.err.println("");
            System.err.println("Program: OpenCGA (OpenCB)");
            System.err.println("Version: 0.6.0");
            System.err.println("");
            System.err.println("Usage:   hpg-bigdata.sh [-h|--help] [--version] <command> <subcommand> [options]");
            System.err.println("");
            System.err.println("Commands:");
            printCommandUsage(jcommander);
            System.err.println("");
        } else {
            String parsedCommand = getCommand();
            if(getSubCommand().isEmpty()){
                System.err.println("");
                System.err.println("Usage:   hpg-bigdata.sh " + parsedCommand + " <subcommand> [options]");
                System.err.println("");
                System.err.println("Subcommands:");
                printCommandUsage(jcommander.getCommands().get(getCommand()));
                System.err.println("");
            } else {
                String parsedSubCommand = getSubCommand();
                System.err.println("");
                System.err.println("Usage:   hpg-bigdata.sh " + parsedCommand + " " + parsedSubCommand + " [options]");
                System.err.println("");
                System.err.println("Options:");
                printSubCommandUsage(jcommander.getCommands().get(parsedCommand).getCommands().get(parsedSubCommand));
                System.err.println("");
            }
        }
    }

    private void printCommandUsage(JCommander commander) {
        int gap = 10;
        int leftGap = 1;
        for (String s : commander.getCommands().keySet()) {
            if (gap < s.length() + leftGap) {
                gap = s.length() + leftGap;
            }
        }
        for (Map.Entry<String, JCommander> entry : commander.getCommands().entrySet()) {
            System.err.printf("%" + gap + "s    %s\n", entry.getKey(), commander.getCommandDescription(entry.getKey()));
        }
    }

    private void printSubCommandUsage(JCommander commander) {
        for (ParameterDescription parameterDescription : commander.getParameters()) {
            String type = "";
            if (parameterDescription.getParameterized().getParameter().arity() > 0) {
                type = parameterDescription.getParameterized().getGenericType().getTypeName().replace("java.lang.", "").toUpperCase();
            }
            System.err.printf("%5s %-20s %-10s %s [%s]\n",
                    parameterDescription.getParameterized().getParameter().required() ? "*": "",
                    parameterDescription.getNames(),
                    type,
                    parameterDescription.getDescription(),
                    parameterDescription.getDefault());
        }
    }


    public GeneralOptions getGeneralOptions() {
        return generalOptions;
    }

    public CommandOptions getCommandOptions() {
        return commandOptions;
    }

//    public IndexCommandOptions getIndexCommandOptions() {
//        return indexCommandOptions;
//    }

    public ConvertCommandOptions getConvertCommandOptions() {
        return convertCommandOptions;
    }

    public SequenceCommandOptions getSequenceCommandOptions() {
        return sequenceCommandOptions;
    }

    public AlignmentCommandOptions getAlignmentCommandOptions() {
        return alignmentCommandOptions;
    }

//    public ConvertSequenceCommandOptions getConvertSequenceCommandOptions() {
//        return convertSequenceCommandOptions;
//    }

    public CommonCommandOptions getCommonCommandOptions() {
        return commonCommandOptions;
    }

    public VariantCommandOptions getVariantCommandOptions() {
        return variantCommandOptions;
    }

}
