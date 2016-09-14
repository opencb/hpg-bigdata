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
import org.apache.parquet.hadoop.ParquetWriter;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by imedina on 16/06/15.
 */
public class LocalCliOptionsParser {

    private final JCommander jcommander;

    private final GeneralOptions generalOptions;
    private final CommandOptions commandOptions;

    private final CommonCommandOptions commonCommandOptions;

    private AdminCommandOptions adminCommandOptions;

    // NGS Sequence command and subcommmands
    private SequenceCommandOptions sequenceCommandOptions;

    // NGS Alignments command and subcommmands
    private AlignmentCommandOptions alignmentCommandOptions;

    // NGS variant command and subcommmands
    private VariantCommandOptions variantCommandOptions;

    public LocalCliOptionsParser() {
        generalOptions = new GeneralOptions();
        jcommander = new JCommander(generalOptions);

        commandOptions = new CommandOptions();
        commonCommandOptions = new CommonCommandOptions();

        adminCommandOptions = new AdminCommandOptions();
        jcommander.addCommand("admin", adminCommandOptions);
        JCommander adminSubCommands = jcommander.getCommands().get("admin");
        adminSubCommands.addCommand("server", adminCommandOptions.serverAdminCommandOptions);


        sequenceCommandOptions = new SequenceCommandOptions();
        jcommander.addCommand("sequence", sequenceCommandOptions);
        JCommander sequenceSubCommands = jcommander.getCommands().get("sequence");
        sequenceSubCommands.addCommand("convert", sequenceCommandOptions.convertSequenceCommandOptions);
        sequenceSubCommands.addCommand("stats", sequenceCommandOptions.statsSequenceCommandOptions);

        alignmentCommandOptions = new AlignmentCommandOptions();
        jcommander.addCommand("alignment", sequenceCommandOptions);
        JCommander alignmentSubCommands = jcommander.getCommands().get("alignment");
        alignmentSubCommands.addCommand("convert", alignmentCommandOptions.convertAlignmentCommandOptions);
        alignmentSubCommands.addCommand("view", alignmentCommandOptions.viewAlignmentCommandOptions);
        alignmentSubCommands.addCommand("sort", alignmentCommandOptions.sortAlignmentCommandOptions);
        alignmentSubCommands.addCommand("stats", alignmentCommandOptions.statsAlignmentCommandOptions);
        alignmentSubCommands.addCommand("depth", alignmentCommandOptions.depthAlignmentCommandOptions);
        alignmentSubCommands.addCommand("query", alignmentCommandOptions.queryAlignmentCommandOptions);

        variantCommandOptions = new VariantCommandOptions();
        jcommander.addCommand("variant", sequenceCommandOptions);
        JCommander variantSubCommands = jcommander.getCommands().get("variant");
        variantSubCommands.addCommand("convert", variantCommandOptions.convertVariantCommandOptions);
        variantSubCommands.addCommand("annotate", variantCommandOptions.annotateVariantCommandOptions);
        variantSubCommands.addCommand("view", variantCommandOptions.viewVariantCommandOptions);
        variantSubCommands.addCommand("query", variantCommandOptions.queryVariantCommandOptions);

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

        @Parameter(names = {"-L", "--log-level"}, description = "Set the level log, values: debug, info, warning, error, fatal", required = false, arity = 1)
        public String logLevel = "info";

        @Deprecated
        @Parameter(names = {"-v", "--verbose"}, description = "This parameter set the level of the logging", required = false, arity = 1)
        public boolean verbose;

        @Parameter(names = {"--conf"}, description = "Set the configuration file", required = false, arity = 1)
        public String conf;

    }

    /*
 * Sequence (FASTQ) CLI options
 */
    @Parameters(commandNames = {"admin"}, commandDescription = "Implements different tools for working with Fastq files")
    public class AdminCommandOptions extends CommandOptions {

        ServerAdminCommandOptions serverAdminCommandOptions;

        public AdminCommandOptions() {
            this.serverAdminCommandOptions = new ServerAdminCommandOptions();
        }
    }

    @Parameters(commandNames = {"server"}, commandDescription = "Converts FastQ files to different big data formats such as Avro")
    class ServerAdminCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-p", "--port"}, description = "Input file name, usually a gVCF/VCF but it can be an Avro file when converting to Parquet.",
                required = true, arity = 1)
        public int port = 1042;

    }



    /*
     * Sequence (FASTQ) CLI options
     */
    @Parameters(commandNames = {"sequence"}, commandDescription = "Implements different tools for working with Fastq files")
    public class SequenceCommandOptions extends CommandOptions {

        ConvertSequenceCommandOptions convertSequenceCommandOptions;
        StatsSequenceCommandOptions statsSequenceCommandOptions;

        public SequenceCommandOptions() {
            this.convertSequenceCommandOptions = new ConvertSequenceCommandOptions();
            this.statsSequenceCommandOptions = new StatsSequenceCommandOptions();
        }
    }

    @Parameters(commandNames = {"convert"}, commandDescription = "Converts FastQ files to different big data formats such as Avro")
    class ConvertSequenceCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"}, description = "Local input file in FastQ format", required = true, arity = 1)
        public String input = null;

        @Parameter(names = {"-o", "--output"}, description = "Local output file to store the FastQ sequences according to the GA4GH/Avro model", required = true, arity = 1)
        public String output = null;

        @Parameter(names = {"-x", "--compression"}, description = "Accepted values: snappy, deflate, bzip2, xz, null. Default: snappy", required = false, arity = 1)
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

        @Parameter(names = {"-i", "--input"}, description = "Local input file containing the FastQ sequences stored according to the GA4GH/Avro model)", required = true, arity = 1)
        public String input = null;

        @Parameter(names = {"-o", "--output"}, description = "Local output directory to save stats results in JSON format ", required = true, arity = 1)
        public String output = null;

        //@Parameter(names = {"-f", "--filter"}, description = "", required = false, arity = 1)
        //public String filter = null;

        @Parameter(names = {"-k", "--kmers"}, description = "Compute k-mers (according to the indicated length)", required = false, arity = 1)
        public Integer kmers = 0;
    }


    /*
     * Alignment (BAM) CLI options
     */
    @Parameters(commandNames = {"alignment"}, commandDescription = "Implements different tools for working with BAM files")
    public class AlignmentCommandOptions extends CommandOptions {

        ConvertAlignmentCommandOptions convertAlignmentCommandOptions;
        ViewAlignmentCommandOptions viewAlignmentCommandOptions;
        SortAlignmentCommandOptions sortAlignmentCommandOptions;
        StatsAlignmentCommandOptions statsAlignmentCommandOptions;
        DepthAlignmentCommandOptions depthAlignmentCommandOptions;
        QueryAlignmentCommandOptions queryAlignmentCommandOptions;

        public AlignmentCommandOptions() {
            this.convertAlignmentCommandOptions = new ConvertAlignmentCommandOptions();
            this.viewAlignmentCommandOptions = new ViewAlignmentCommandOptions();
            this.sortAlignmentCommandOptions = new SortAlignmentCommandOptions();
            this.statsAlignmentCommandOptions = new StatsAlignmentCommandOptions();
            this.depthAlignmentCommandOptions = new DepthAlignmentCommandOptions();
            this.queryAlignmentCommandOptions = new QueryAlignmentCommandOptions();
        }
    }

    @Parameters(commandNames = {"convert"}, commandDescription = "Converts BAM files to different big data formats such as Avro")
    class ConvertAlignmentCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"}, description = "Input filename in BAM format", required = true, arity = 1)
        public String input = null;

        @Parameter(names = {"-o", "--output"}, description = "Output filename to store the BAM alignments according to the GA4GH/Avro model", required = true, arity = 1)
        public String output = null;

        @Parameter(names = {"-x", "--compression"}, description = "Compression method, accepted values: gzip, snappy", arity = 1)
        public String compression = "gzip";

        @Parameter(names = {"--to"}, description = "Destination format, accepted values: avro, parquet", arity = 1)
        public String to = "avro";

//        @Parameter(names = {"--to-bam"}, description = "Convert back to BAM format. In this case, the input file has to be saved in the GA4GH/Avro model, and the output file will be in BAM format", required = false)
//        public boolean toBam;

        @Parameter(names = {"--bin-qualities"}, description = "Compress the nucleotide qualities by using 8 quality levels (there will be loss of information)")
        public boolean binQualities = false;

        @Parameter(names = {"--min-mapq"}, description = "Minimum mapping quality", arity = 1)
        public int minMapQ = 20;

        @Parameter(names = {"--region"}, description = "Comma separated list of regions, e.g.: 1:300000-400000000,15:343453463-8787665654", arity = 1)
        public String regions = null;

        @Parameter(names = {"--region-file"}, description = "Input filename with a list of regions. One region per line, e.g.: 1:300000-400000000", arity = 1)
        public String regionFilename = null;

        @Parameter(names = {"--page-size"}, description = "Page size, only for parquet conversions", arity = 1)
        public int pageSize = ParquetWriter.DEFAULT_PAGE_SIZE;

        @Parameter(names = {"--block-size"}, description = "Block size, only for parquet conversions", arity = 1)
        public int blockSize = ParquetWriter.DEFAULT_BLOCK_SIZE;
    }

    @Parameters(commandNames = {"view"}, commandDescription = "Display alignment Avro/Parquet files")
    class ViewAlignmentCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"}, description = "Input file name (Avro/Parquet format)",
                required = true, arity = 1)
        public String input;

        @Parameter(names = {"--head"}, description = "Output the first alignments of the file",
                arity = 1)
        public int head;

        @Parameter(names = {"--schema"}, description = "Display the Avro schema")
        public boolean schema = false;
    }

    @Parameters(commandNames = {"sort"}, commandDescription = "Sort alignments of a GA4GH/Avro file")
    class SortAlignmentCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"}, description = "Input filename containing alignments to sort (GA4GH/Avro format)", required = true, arity = 1)
        public String input = null;

        @Parameter(names = {"-o", "--output"}, description = "Output filename to store the alignments sorted (GA4GH/Avro format)", required = true, arity = 1)
        public String output = null;
    }

    @Parameters(commandNames = {"stats"}, commandDescription = "Compute some stats for a file containing alignments according to the GA4GH/Avro model")
    class StatsAlignmentCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"}, description = "Local input file containing alignments stored according to the GA4GH/Avro model)", required = true, arity = 1)
        public String input = null;

        @Parameter(names = {"-o", "--output"}, description = "Local output directory to save stats results in JSON format ", required = true, arity = 1)
        public String output = null;

        //@Parameter(names = {"-f", "--filter"}, description = "", required = false, arity = 1)
        //public String filter = null;
    }

    @Parameters(commandNames = {"depth"}, commandDescription = "Compute the depth (or coverage) for a given file containing alignments according to the GA4GH/Avro model")
    class DepthAlignmentCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"}, description = "Local input file containing alignments stored according to the GA4GH/Avro model. This file must be sorted", required = true, arity = 1)
        public String input = null;

        @Parameter(names = {"-o", "--output"}, description = "Local output directory to save the depth in a text file", required = true, arity = 1)
        public String output = null;

        @Parameter(names = {"--region"}, description = "Comma separated list of regions, e.g.: 1:300000-400000000,15:343453463-8787665654", arity = 1)
        public String regions = null;

        @Parameter(names = {"--region-file"}, description = "Input filename with a list of regions. One region per line, e.g.: 1:300000-400000000", arity = 1)
        public String regionFilename = null;
    }

    @Parameters(commandNames = {"query"}, commandDescription = "Command to execute queries against the Alignment input file (Avro or Parquet), the results will be saved into the output file.")
    class QueryAlignmentCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"}, description = "Input file name (in Avro or Parquet format) that contains the alignments.",
                required = true, arity = 1)
        public String input;

        @Parameter(names = {"-o", "--output"}, description = "Output file name.",
                required = true, arity = 1)
        public String output;

        @Parameter(names = {"--region"}, description = "Query for region; comma separated list of regions, e.g.: 1:300000-400000000,15:343453463-8787665654", required = false)
        public String regions;

        @Parameter(names = {"--region-file"}, description = "Query for region; the list of regions is stored in this input file, one region (1:300000-400000000) per line", required = false)
        public String regionFile;

        @Parameter(names = {"--min-mapq"}, description = "Query for minimun mappging quality",
                required = false, arity = 1)
        public int minMapQ = 0;

        @Parameter(names = {"--require-flags"}, description = "Query for alignments matching theses flags",
                required = false, arity = 1)
        public int requireFlags = 4095;

        @Parameter(names = {"--filtering-flags"}, description = "Query for alignments not matching these flags",
                required = false, arity = 1)
        public int filteringFlags = 0;

        @Parameter(names = {"--min-tlen"}, description = "Query for alignments with a template length greater than the minimun",
                required = false, arity = 1)
        public int minTLen = 0;

        @Parameter(names = {"--max-tlen"}, description = "Query for alignments with a template length less than the maximum\"",
                required = false, arity = 1)
        public int maxTLen = Integer.MAX_VALUE;

        @Parameter(names = {"--min-alen"}, description = "Query for alignments with an alignment length greater than the minimun",
                required = false, arity = 1)
        public int minALen = 0;

        @Parameter(names = {"--max-alen"}, description = "Query for alignments with an alignment length less than the maximum\"",
                required = false, arity = 1)
        public int maxALen = Integer.MAX_VALUE;
    }


    /*
     * Variant (VCF) CLI options
     */
    @Parameters(commandNames = {"variant"}, commandDescription = "Implements different tools for working with gVCF/VCF files")
    public class VariantCommandOptions extends CommandOptions {

        ConvertVariantCommandOptions convertVariantCommandOptions;
        AnnotateVariantCommandOptions annotateVariantCommandOptions;
        ViewVariantCommandOptions viewVariantCommandOptions;
        QueryVariantCommandOptions queryVariantCommandOptions;

        public VariantCommandOptions() {
            this.convertVariantCommandOptions = new ConvertVariantCommandOptions();
            this.annotateVariantCommandOptions = new AnnotateVariantCommandOptions();
            this.viewVariantCommandOptions = new ViewVariantCommandOptions();
            this.queryVariantCommandOptions = new QueryVariantCommandOptions();
        }
    }

    @Parameters(commandNames = {"convert"}, commandDescription = "Convert gVCF/VCF files to different big data formats such as Avro and Parquet using OpenCB models")
    class ConvertVariantCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"}, description = "Input gVCF/VCF file name",
//        @Parameter(names = {"-i", "--input"}, description = "Input file name, usually a gVCF/VCF but it can be an Avro file when converting to Parquet.",
                required = true, arity = 1)
        public String input;

        @Parameter(names = {"--to"}, description = "Destination format, accepted values: avro, parquet", arity = 1)
        public String to = "avro";

        @Parameter(names = {"-o", "--output"}, description = "Output file name", required = true, arity = 1)
        public String output = null;

//        @Parameter(names = {"-O"}, description = "Use the standard output.", required = false, arity = 0)
//        public boolean stdOutput;

//        @Parameter(names = {"--from"}, description = "Accepted values: vcf, avro", required = false)
//        public String from;

//        @Parameter(names = {"-d", "--data-model"}, description = "Only for 'to-json' and 'to-avro' options. 'to-protobuf' is only available with opencb data models. Values: opencb, ga4gh", required = false, arity = 1)
//        public String dataModel = "opencb";

//        @Parameter(names = {"-x", "--compression"}, description = "Available options for Avro are: : snappy, deflate, bzip2, xz. " +
//                "For JSON and ProtoBuf only 'gzip' is available. It compression is null,  it will be inferred compression from file extensions: .gz, .sz, ...", required = false, arity = 1)
//        public String compression = "deflate";

        @Parameter(names = {"-x", "--compression"}, description = "Compression method, acepted values: : snappy, gzip, bzip2, xz", arity = 1)
        public String compression = "gzip";

        @Parameter(names = {"--only-with-id"}, description = "Variants with IDs (variants with missing IDs will be discarded)")
        public boolean validId = false;

//        @Parameter(names = {"--min-quality"}, description = "Minimum quality", arity = 1)
//        public int minQuality = 20;

        @Parameter(names = {"--region"}, description = "Comma separated list of regions, e.g.: 1:300000-400000000,15:343453463-8787665654", arity = 1)
        public String regions = null;

        @Parameter(names = {"--region-file"}, description = "Input filename with a list of regions. One region per line, e.g.: 1:300000-400000000", arity = 1)
        public String regionFilename = null;

        @Parameter(names = {"--page-size"}, description = "Page size, only for parquet conversions", arity = 1)
        public int pageSize = ParquetWriter.DEFAULT_PAGE_SIZE;

        @Parameter(names = {"--block-size"}, description = "Block size, only for parquet conversions", arity = 1)
        public int blockSize = ParquetWriter.DEFAULT_BLOCK_SIZE;

//        @Parameter(names = {"-t", "--num-threads"}, description = "Number of threads to use, this must be less than the number of cores", required = false)
//        public int numThreads = 1;

//        @Parameter(names = {"--skip-normalization"}, description = "Whether to skip variant normalization", required = false)
//        public boolean skipNormalization;

//        @DynamicParameter(names = {"-D"}, hidden = true)
//        public Map<String, String> options = new HashMap<>();
    }


    @Parameters(commandNames = {"annotate"}, commandDescription = "Convert gVCF/VCF files to different big data formats such as Avro and Parquet using GA4GH models")
    class AnnotateVariantCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"}, description = "0000 Input file name, usually a gVCF/VCF but it can be an Avro file when converting to Parquet.",
                required = true, arity = 1)
        public String input;

        @Parameter(names = {"-o", "--output"}, description = "Input file name, usually a gVCF/VCF but it can be an Avro file when converting to Parquet.",
                required = true, arity = 1)
        public String ouput;
    }

    @Parameters(commandNames = {"view"}, commandDescription = "Display variant Avro/Parquet files")
    class ViewVariantCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"}, description = "Input file name (Avro/Parquet format)",
                required = true, arity = 1)
        public String input;

        @Parameter(names = {"--head"}, description = "Output the first variants of the file",
                arity = 1)
        public int head;

        @Parameter(names = {"--schema"}, description = "Display the Avro schema")
        public boolean schema = false;
    }

    @Parameters(commandNames = {"query"}, commandDescription = "Command to execute queries against the input file (Avro or Parquet) saving the results in the output file.")
    class QueryVariantCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"}, description = "Input file name (in Avro or Parquet format).",
                required = true, arity = 1)
        public String input;

        @Parameter(names = {"-o", "--output"}, description = "Output file name.",
                required = true, arity = 1)
        public String output;

        @Parameter(names = {"--id"}, description = "Query for ID; comma separated list of IDs, e.g.: \"rs312411,rs421225\"",
                arity = 1)
        public String ids;

        @Parameter(names = {"--id-file"}, description = "Query for ID that are stored in a file, one ID per line, e.g.: rs312411",
                arity = 1)
        public String idFilename;

        @Parameter(names = {"--type"}, description = "Query for type; comma separated list of IDs, e.g.: \"INDEL,SNP,SNV\"",
                arity = 1)
        public String types;

        @Parameter(names = {"--s", "--study"}, description = "Query for study; comma separated list of study names",
                arity = 1)
        public String studies;

        @Parameter(names = {"--biotype"}, description = "Query for biotype; comma separated list of biotype names, e.g.: protein_coding, pseudogene",
                arity = 1)
        public String biotypes;

        @Parameter(names = {"-r", "--region"}, description = "Query for region; comma separated list of regions, e.g.: 1:300000-400000000,15:343453463-8787665654",
                arity = 1)
        public String regions;

        @Parameter(names = {"--region-file"}, description = "Query for regions that are stored in a file, one region per line,  e.g.: 1:6700000-560000000",
                arity = 1)
        public String regionFilename;

        @Parameter(names = {"--maf"}, description = "Query for the Minor Allele Frequency of a given study and cohort. Use the following format enclosed with double quotes: \"study_name::cohort_name[<|>|<=|>=|==|!=]value\", e.g.: \"1000g::all>0.4\"",
                arity = 1)
        public String maf;

        @Parameter(names = {"--mgf"}, description = "Query for the Minor Genotype Frequency of a given study and cohort. Use the following format enclosed with double quotes: \"study_name::cohort_name[<|>|<=|>=|==|!=]value\", e.g.: \"1000g::all>0.18198\"",
                arity = 1)
        public String mgf;

        @Parameter(names = {"--missing-allele"}, description = "Query for the number of missing alleles of a given study and cohort. Use the following format enclosed with double quotes: \"study_name::cohort_name[<|>|<=|>=|==|!=]value\", e.g.: \"1000g::all==5\"",
                arity = 1)
        public String missingAlleles;

        @Parameter(names = {"--missing-genotype"}, description = "Query for the number of missing genotypes of a given study and cohort. Use the following format enclosed with double quotes: \"study_name::cohort_name[<|>|<=|>=|==|!=]value\", e.g.: \"1000g::all!=0\"",
                arity = 1)
        public String missingGenotypes;

        @Parameter(names = {"--ct", "--consequence-type"}, description = "Query for Sequence Ontology term names or accession codes; comma separated (use double quotes if you provide term names), e.g.: \"transgenic insertion,SO:32234,SO:00124\"",
                arity = 1)
        public String consequenceTypes;

        @Parameter(names = {"--clinvar"}, description = "Query for clinvar (accession); comma separated list of accessions", arity = 1)
        public String clinvar;

        @Parameter(names = {"--cosmic"}, description = "Query for cosmic (mutation ID); comma separated list of mutations IDs", arity = 1)
        public String cosmic;

//        @Parameter(names = {"--gwas"}, description = "Query for gwas (traits); comma separated list of traits",  arity = 1)
//        public String gwas;

        @Parameter(names = {"--conservation"}, description = "Query for conservation scores (phastCons, phylop, gerp); comma separated list of scores and enclosed with double quotes, e.g.: \"phylop<0.3,phastCons<0.1\"",
                arity = 1)
        public String conservScores;

        @Parameter(names = {"--ps", "--protein-substitution"}, description = "Query for protein substitution scores (polyphen, sift); comma separated list of scores and enclosed with double quotes, e.g.: \"polyphen>0.3,sift>0.6\"",
                arity = 1)
        public String substScores;

        @Parameter(names = {"--pf", "--population-frequency"}, description = "Query for alternate population frequency of a given study. Use the following format enclosed with double quotes: \"study_name::population_name[<|>|<=|>=|==|!=]frequency_value\", e.g.: \"1000g::CEU<0.4\"",
                arity = 1)
        public String pf;

        @Parameter(names = {"--pmaf", "--population-maf"}, description = "Query for population minor allele frequency of a given study. Use the following the format enclosed with double quotes: \"study_name::population_name[<|>|<=|>=|==|!=]frequency_value\", e.g.: \"1000g::PJL<=0.25\"",
                arity = 1)
        public String pmaf;

        @Parameter(names = {"--show"}, description = "Display the first output records", arity = 1)
        public int show = 0;

        @Parameter(names = {"--count"}, description = "Display the number of output records")
        public boolean count = false;
    }


    public void printUsage(){
        if(getCommand().isEmpty()) {
            System.err.println("");
            System.err.println("Program:     HPG BigData for HPC (OpenCB)");
            System.err.println("Version:     0.2.0");
            System.err.println("Description: Tools for working with NGS data in a standard HPC cluster");
            System.err.println("");
            System.err.println("Usage:       hpg-bigdata-local.sh [-h|--help] [--version] <command> <subcommand> [options]");
            System.err.println("");
            System.err.println("Commands:");
            printMainUsage();
            System.err.println("");
        } else {
            String parsedCommand = getCommand();
            if(getSubCommand().isEmpty()){
                System.err.println("");
                System.err.println("Usage:   hpg-bigdata-local.sh " + parsedCommand + " <subcommand> [options]");
                System.err.println("");
                System.err.println("Subcommands:");
                printCommandUsage(jcommander.getCommands().get(getCommand()));
                System.err.println("");
            } else {
                String parsedSubCommand = getSubCommand();
                System.err.println("");
                System.err.println("Usage:   hpg-bigdata-local.sh " + parsedCommand + " " + parsedSubCommand + " [options]");
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

    public AdminCommandOptions getAdminCommandOptions() {
        return adminCommandOptions;
    }

    public SequenceCommandOptions getSequenceCommandOptions() {
        return sequenceCommandOptions;
    }

    public AlignmentCommandOptions getAlignmentCommandOptions() {
        return alignmentCommandOptions;
    }

    public CommonCommandOptions getCommonCommandOptions() {
        return commonCommandOptions;
    }

    public VariantCommandOptions getVariantCommandOptions() {
        return variantCommandOptions;
    }

}
