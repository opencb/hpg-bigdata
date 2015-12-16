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

import java.util.HashMap;
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
        //sequenceSubCommands.addCommand("align", sequenceCommandOptions.alignSequenceCommandOptions);

        alignmentCommandOptions = new AlignmentCommandOptions();
        jcommander.addCommand("alignment", sequenceCommandOptions);
        JCommander alignmentSubCommands = jcommander.getCommands().get("alignment");
        alignmentSubCommands.addCommand("convert", alignmentCommandOptions.convertAlignmentCommandOptions);
        alignmentSubCommands.addCommand("stats", alignmentCommandOptions.statsAlignmentCommandOptions);
        alignmentSubCommands.addCommand("depth", alignmentCommandOptions.depthAlignmentCommandOptions);

        variantCommandOptions = new VariantCommandOptions();
        jcommander.addCommand("variant", sequenceCommandOptions);
        JCommander variantSubCommands = jcommander.getCommands().get("variant");
        variantSubCommands.addCommand("convert", variantCommandOptions.convertVariantCommandOptions);
        variantSubCommands.addCommand("index", variantCommandOptions.indexVariantCommandOptions);

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

        @Parameter(names = {"-h", "--help"},  description = "This parameter prints this help", help = true)
        public boolean help;

        @Parameter(names = {"--version"})
        public boolean version;

    }

    /**
     * This class contains all those parameters available for all 'commands'
     */
    public class CommandOptions {

        @Parameter(names = {"-h", "--help"},  description = "This parameter prints this help", help = true)
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

        @Parameter(names = {"-h", "--help"},  description = "This parameter prints this help", help = true)
        public boolean help;

        @Parameter(names = {"-L", "--log-level"},
                description = "Set the level log, values: debug, info, warning, error, fatal",
                required = false, arity = 1)
        public String logLevel = "info";

        @Deprecated
        @Parameter(names = {"-v", "--verbose"},
        description = "This parameter set the level of the logging", required = false, arity = 1)
        public boolean verbose;

        @Parameter(names = {"--conf"}, description = "Set the configuration file", required = false, arity = 1)
        public String conf;

    }


    /*
     * Sequence (FASTQ) CLI options
     */
    @Parameters(commandNames = {"sequence"},
            commandDescription = "Implements different tools for working with Fastq files")
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

    @Parameters(commandNames = {"convert"},
            commandDescription = "Converts FastQ files to different big data formats such as Avro")
    class ConvertSequenceCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"},
                description = "HDFS input file in FastQ format", required = true, arity = 1)
        public String input = null;

        @Parameter(names = {"-o", "--output"},
                description = "HDFS output file to store the FastQ sequences according to the GA4GH/Avro model",
                required = true, arity = 1)
        public String output = null;

        @Parameter(names = {"-x", "--compression"},
                description = "Accepted values: snappy, deflate, bzip2, xz, null. Default: snappy",
                required = false, arity = 1)
        public String compression = "snappy";

        //@Parameter(names = {"--to-avro"}, description = "", required = false)
        //public boolean toAvro = true;

        //@Parameter(names = {"--to-fastq"}, description = "", required = false)
        //public boolean toFastq;
    }

    @Parameters(commandNames = {"stats"}, commandDescription = "Calculates different stats from sequencing data")
    class StatsSequenceCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"},
                description = "HDFS input file containing the FastQ sequences stored in GA4GH/Avro model)",
                required = true, arity = 1)
        public String input = null;

        @Parameter(names = {"-o", "--output"},
                description = "Local output directory to save stats results in JSON format ",
                required = true, arity = 1)
        public String output = null;

        //@Parameter(names = {"-f", "--filter"}, description = "", required = false, arity = 1)
        //public String filter = null;

        @Parameter(names = {"-k", "--kmers"},
                description = "Compute k-mers (according to the indicated length)", required = false, arity = 1)
        public Integer kmers = 0;
    }

    @Parameters(commandNames = {"align"},
            commandDescription = "Align reads to a reference genome using HPG Aligner in MapReduce")
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
    @Parameters(commandNames = {"alignment"},
            commandDescription = "Implements different tools for working with BAM files")
    public class AlignmentCommandOptions extends CommandOptions {

        ConvertAlignmentCommandOptions convertAlignmentCommandOptions;
        StatsAlignmentCommandOptions statsAlignmentCommandOptions;
        DepthAlignmentCommandOptions depthAlignmentCommandOptions;

        public AlignmentCommandOptions() {
            this.convertAlignmentCommandOptions = new ConvertAlignmentCommandOptions();
            this.statsAlignmentCommandOptions = new StatsAlignmentCommandOptions();
            this.depthAlignmentCommandOptions = new DepthAlignmentCommandOptions();
        }
    }

    @Parameters(commandNames = {"convert"},
            commandDescription = "Converts BAM files to different big data formats such as Avro and Parquet")
    class ConvertAlignmentCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"}, description = "HDFS input file in BAM format", required = true, arity = 1)
        public String input = null;

        @Parameter(names = {"-o", "--output"},
                description = "HDFS output file to store the BAM alignments according to the GA4GH/Avro model",
                required = true, arity = 1)
        public String output = null;

        @Parameter(names = {"-x", "--compression"},
                description = "Accepted values: snappy, deflate, bzip2, xz, null. Default: snappy",
                required = false, arity = 1)
        public String compression = "snappy";

        //@Parameter(names = {"--to-avro"}, description = "", required = false)
        //public boolean toAvro;

        @Parameter(names = {"--to-parquet"}, description = "To save the output file in Parquet", required = false)
        public boolean toParquet;

        //@Parameter(names = {"--to-fastq"}, description = "", required = false)
        //public boolean toFastq;

        @Parameter(names = {"--adjust-quality"},
                description = "Compress quality field using 8 quality levels. Will loss information", required = false)
        public boolean adjustQuality;
    }

    @Parameters(commandNames = {"stats"},
            commandDescription = "Compute some stats for a file containing alignments in GA4GH/Avro model")
    class StatsAlignmentCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"},
                description = "HDFS input file containing alignments stored according to the GA4GH/Avro model)",
                required = true, arity = 1)
        public String input = null;

        @Parameter(names = {"-o", "--output"},
                description = "Local output directory to save stats results in JSON format", required = true, arity = 1)
        public String output = null;

        //@Parameter(names = {"-f", "--filter"}, description = "", required = false, arity = 1)
        //public String filter = null;
    }

    @Parameters(commandNames = {"depth"},
            commandDescription = "Compute the depth for a given file containing alignments in GA4GH/Avro model")
    class DepthAlignmentCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"},
                description = "HDFS input file containing alignments stored according to the GA4GH/Avro model)",
                required = true, arity = 1)
        public String input = null;

        @Parameter(names = {"-o", "--output"},
                description = "Local output directory to save the depth in a text file", required = true, arity = 1)
        public String output = null;

        @Parameter(names = {"-r", "--regions"}, description = "Compute depth for the mentioned regions separated by commas. The region format is: chromosome:start-end. Example: 3:230000000-250000000,12:435050000-435100000", required = false, arity = 1)
        public String regions = null;

        @Parameter(names = {"-q", "--min-mapq"}, description = "Compute depth for alignments whose mapping quality is greater that this minimum mapping quality", required = false, arity = 1)
        public int minMapQ = 0;
    }

    /*
     * Variant (VCF) CLI options
     */
    @Parameters(commandNames = {"variant"},
            commandDescription = "Implements different tools for working with gVCF/VCF files")
    public class VariantCommandOptions extends CommandOptions {

        ConvertVariantCommandOptions convertVariantCommandOptions;
        IndexVariantCommandOptions indexVariantCommandOptions;

        public VariantCommandOptions() {
            this.convertVariantCommandOptions = new ConvertVariantCommandOptions();
            this.indexVariantCommandOptions = new IndexVariantCommandOptions();
        }
    }

    @Parameters(commandNames = {"convert"},
            commandDescription = "Convert gVCF/VCF files to different data formats such as Avro/Parquet")
    class ConvertVariantCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"},
                description = "HDFS input file (the BAM/SAM file must be stored in GA4GH/Avro model)",
                required = true, arity = 1)
        public String input = null;

        @Parameter(names = {"-o", "--output"},
                description = "Local output directory to save results, summary, images...", required = false, arity = 1)
        public String output = null;

        @Parameter(names = {"--to-avro"},
                description = "Accepted values: sam2bam, sam2cram, bam2fastq", required = false)
        public boolean toAvro;

        @Parameter(names = {"--from-avro"}, description = "Converts Avro format into JSON", required = false)
        public boolean fromAvro;

        @Parameter(names = {"-x", "--compression"},
                description = "Only for commands 'to-avro' and 'to-parquet'. Values: snappy, deflate, bzip2, xz",
                required = false, arity = 1)
        public String compression = "snappy";

        @Parameter(names = {"--to-parquet"},
                description = "Accepted values: sam2bam, sam2cram, bam2fastq", required = false)
        public boolean toParquet;

    }

    @Parameters(commandNames = {"index"},
            commandDescription = "Load avro gVCF/VCF files into different NoSQL, only HBase implemented so far")
    public class IndexVariantCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"}, description = "Input file can be .vcf.gz or avro", required = true, arity = 1)
        public String input = null;

        @Parameter(names = {"-t", "--type"}, description = "Type can be: vcf, bed, or gff", arity = 1)
        public String type = "vcf";

        @Parameter(names = {"-lt", "--loadtype"}, description = "Load can be overwrite/force delete", arity = 1)
        public String loadtype = "overwrite";

        @Parameter(names = {"-se", "--storage-engine"},
                description = "Database, values: hbase, hive, impala", arity = 1)
        public String database = "hbase";

        @Parameter(names = {"-r", "--regions"}, description = "Database to load data, values: hbase", arity = 1)
        public String regions = null;

        @Parameter(names = {"-g", "--genome"}, description = "Load whole genome from gVCF - including non-variant regions", required = false, arity = 1)
        public boolean includeNonVariants = false;

        @Parameter(names = {"-e", "--expand"}, description = "Expand non-variant gVCF regions to one entry per base", required = false, arity = 1)
        public boolean expand = false;

        @Parameter(names = {"--credentials"}, description = "Database credentials: user, password, host, port", arity = 1)
        public String credentials;

        @Parameter(names = {"--hdfspath"},
                description = "HDFS Path", arity = 1)
        public String hdfsPath;

    }


    public void printUsage(){
        if(getCommand().isEmpty()) {
            System.err.println("");
            System.err.println("Program:     HPG BigData for Hadoop (OpenCB)");
            System.err.println("Version:     0.2.0");
            System.err.println("Description: Hadoop-based tools for working with NGS data");
            System.err.println("");
            System.err.println("Usage:       hpg-bigdata.sh [-h|--help] [--version] <command> <subcommand> [options]");
            System.err.println("");
            System.err.println("Commands:");
            printMainUsage();
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

    private void printMainUsage() {
        // TODO This is a nasty hack. By some unknown reason JCommander only prints the description from first command
        Map<String, String> commandDescription = new HashMap<>();
        commandDescription.put("sequence", "Implements different tools for working with Fastq files");
        commandDescription.put("alignment", "Implements different tools for working with SAM/BAM files");
        commandDescription.put("variant", "Implements different tools for working with gVCF/VCF files");

        for (String s : jcommander.getCommands().keySet()) {
            System.err.printf("%12s  %s\n", s, commandDescription.get(s));
        }
    }

    private void printCommandUsage(JCommander commander) {
        for (Map.Entry<String, JCommander> entry : commander.getCommands().entrySet()) {
            System.err.printf("%12s  %s\n", entry.getKey(), commander.getCommandDescription(entry.getKey()));
        }
    }

    private void printSubCommandUsage(JCommander commander) {
        for (ParameterDescription parameterDescription : commander.getParameters()) {
            String type = "";
            if (parameterDescription.getParameterized().getParameter().arity() > 0) {
                type = parameterDescription.getParameterized().getGenericType().getTypeName()
                        .replace("java.lang.", "").toUpperCase();
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

    public CommonCommandOptions getCommonCommandOptions() {
        return commonCommandOptions;
    }

    public SequenceCommandOptions getSequenceCommandOptions() {
        return sequenceCommandOptions;
    }

    public AlignmentCommandOptions getAlignmentCommandOptions() {
        return alignmentCommandOptions;
    }

    public VariantCommandOptions getVariantCommandOptions() {
        return variantCommandOptions;
    }

}


