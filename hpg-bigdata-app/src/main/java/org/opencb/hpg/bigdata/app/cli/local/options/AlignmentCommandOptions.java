package org.opencb.hpg.bigdata.app.cli.local.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.apache.parquet.hadoop.ParquetWriter;
import org.opencb.hpg.bigdata.app.cli.local.LocalCliOptionsParser;

/**
 * Created by jtarraga on 01/06/17.
 */
@Parameters(commandNames = {"alignment"}, commandDescription = "Implements different tools for working with BAM files")
public class AlignmentCommandOptions {
    /*
     * Alignment (BAM) CLI options
    */

    public ConvertAlignmentCommandOptions convertAlignmentCommandOptions;
    public ViewAlignmentCommandOptions viewAlignmentCommandOptions;
    public SortAlignmentCommandOptions sortAlignmentCommandOptions;
    public StatsAlignmentCommandOptions statsAlignmentCommandOptions;
    public CoverageAlignmentCommandOptions coverageAlignmentCommandOptions;
    public QueryAlignmentCommandOptions queryAlignmentCommandOptions;

    public LocalCliOptionsParser.CommonCommandOptions commonCommandOptions;
    public JCommander jCommander;

    public AlignmentCommandOptions(LocalCliOptionsParser.CommonCommandOptions commonCommandOptions,
                                   JCommander jCommander) {
        this.commonCommandOptions = commonCommandOptions;
        this.jCommander = jCommander;

        this.convertAlignmentCommandOptions = new ConvertAlignmentCommandOptions();
        this.viewAlignmentCommandOptions = new ViewAlignmentCommandOptions();
        this.sortAlignmentCommandOptions = new SortAlignmentCommandOptions();
        this.statsAlignmentCommandOptions = new StatsAlignmentCommandOptions();
        this.coverageAlignmentCommandOptions = new CoverageAlignmentCommandOptions();
        this.queryAlignmentCommandOptions = new QueryAlignmentCommandOptions();
    }

    @Parameters(commandNames = {"convert"}, commandDescription = "Converts BAM files to different big data formats such"
            + " as Avro, Parquet")
    public class ConvertAlignmentCommandOptions {

        @ParametersDelegate
        public LocalCliOptionsParser.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"}, description = "Input filename in BAM format", required = true, arity = 1)
        public String input = null;

        @Parameter(names = {"-o", "--output"}, description = "Output filename to store the BAM alignments according to"
                + " the GA4GH/Avro model", required = true, arity = 1)
        public String output = null;

        @Parameter(names = {"-x", "--compression"}, description = "Compression method, accepted values: gzip, snappy",
                arity = 1)
        public String compression = "gzip";

        @Parameter(names = {"--to"}, description = "Destination format, accepted values: avro, parquet", arity = 1)
        public String to = "avro";

        @Parameter(names = {"--from"}, description = "Input file format, accepted values: bam, avro", arity = 1)
        public String from = "bam";

        @Parameter(names = {"--bin-qualities"}, description = "Compress the nucleotide qualities by using 8 quality"
                + " levels (there will be loss of information)")
        public boolean binQualities = false;

        @Parameter(names = {"--min-mapq"}, description = "Minimum mapping quality", arity = 1)
        public int minMapQ = 20;

        @Parameter(names = {"--region"}, description = "Comma separated list of regions, e.g.: 1:300000-400000000,"
                + "15:343453463-8787665654", arity = 1)
        public String regions = null;

        @Parameter(names = {"--region-file"}, description = "Input filename with a list of regions. One region per"
                + " line, e.g.: 1:300000-400000000", arity = 1)
        public String regionFilename = null;

        @Parameter(names = {"--page-size"}, description = "Page size, only for parquet conversions", arity = 1)
        public int pageSize = ParquetWriter.DEFAULT_PAGE_SIZE;

        @Parameter(names = {"--block-size"}, description = "Block size, only for parquet conversions", arity = 1)
        public int blockSize = ParquetWriter.DEFAULT_BLOCK_SIZE;
    }

    @Parameters(commandNames = {"view"}, commandDescription = "Display alignment Avro/Parquet files")
    public class ViewAlignmentCommandOptions {

        @ParametersDelegate
        public LocalCliOptionsParser.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"}, description = "Input file name (Avro/Parquet format)",
                required = true, arity = 1)
        public String input;

        @Parameter(names = {"--head"}, description = "Output the first alignments of the file",
                arity = 1)
        public int head;

        @Parameter(names = {"--schema"}, description = "Display the Avro schema")
        public boolean schema = false;

        @Parameter(names = {"--exclude-sequences"}, description = "Nucleotide sequences are not displayed")
        public boolean excludeSequences = false;

        @Parameter(names = {"--exclude-qualities"}, description = "Quality sequences are not displayed")
        public boolean excludeQualities = false;

        @Parameter(names = {"--sam"}, description = "Alignments are displayed in SAM format")
        public boolean sam = false;
    }

    @Parameters(commandNames = {"sort"}, commandDescription = "Sort alignments of a GA4GH/Avro file")
    public class SortAlignmentCommandOptions {

        @ParametersDelegate
        public LocalCliOptionsParser.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"}, description = "Input filename containing alignments to sort"
                + " (GA4GH/Avro format)", required = true, arity = 1)
        public String input = null;

        @Parameter(names = {"-o", "--output"}, description = "Output filename to store the alignments sorted"
                + " (GA4GH/Avro format)", required = true, arity = 1)
        public String output = null;
    }

    @Parameters(commandNames = {"stats"}, commandDescription = "Compute some stats for a file containing alignments"
            + " according to the GA4GH/Avro model")
    public class StatsAlignmentCommandOptions {

        @ParametersDelegate
        public LocalCliOptionsParser.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"}, description = "Local input file containing alignments stored according"
                + " to the GA4GH/Avro model)", required = true, arity = 1)
        public String input = null;

        @Parameter(names = {"-o", "--output"}, description = "Local output directory to save stats results in JSON"
                + " format ", required = true, arity = 1)
        public String output = null;

        //@Parameter(names = {"-f", "--filter"}, description = "", required = false, arity = 1)
        //public String filter = null;
    }

    @Parameters(commandNames = {"coverage"}, commandDescription = "Compute the coverage for a given file containing"
            + " alignments according to the GA4GH/Avro model")
    public class CoverageAlignmentCommandOptions {

        @ParametersDelegate
        public LocalCliOptionsParser.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"}, description = "Local input file containing alignments stored according"
                + " to the GA4GH/Avro model. This file must be sorted", required = true, arity = 1)
        public String input = null;

        @Parameter(names = {"-o", "--output"}, description = "Local output directory to save the coverage in a text"
                + " file", required = true, arity = 1)
        public String output = null;

        @Parameter(names = {"--region"}, description = "Comma separated list of regions, e.g.: 1:300000-400000000,"
                + "15:343453463-8787665654", arity = 1)
        public String regions = null;

        @Parameter(names = {"--region-file"}, description = "Input filename with a list of regions. One region per"
                + " line, e.g.: 1:300000-400000000", arity = 1)
        public String regionFilename = null;
    }

    @Parameters(commandNames = {"query"}, commandDescription = "Command to execute queries against the Alignment input"
            + " file (Avro or Parquet), the results will be saved into the output file.")
    public class QueryAlignmentCommandOptions {

        @ParametersDelegate
        public LocalCliOptionsParser.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"}, description = "Input file name (in Avro or Parquet format) that contains"
                + " the alignments.", required = true, arity = 1)
        public String input;

        @Parameter(names = {"-o", "--output"}, description = "Output file name.",
                required = true, arity = 1)
        public String output;

        @Parameter(names = {"--region"}, description = "Query for region; comma separated list of regions, e.g.:"
                + " 1:300000-400000000,15:343453463-8787665654")
        public String regions;

        @Parameter(names = {"--region-file"}, description = "Query for region; the list of regions is stored in this"
                + " input file, one region (1:300000-400000000) per line")
        public String regionFile;

        @Parameter(names = {"--min-mapq"}, description = "Query for minimun mappging quality", arity = 1)
        public int minMapQ = 0;

        @Parameter(names = {"--require-flags"}, description = "Query for alignments matching theses flags", arity = 1)
        public int requireFlags = 4095;

        @Parameter(names = {"--filtering-flags"}, description = "Query for alignments not matching these flags",
                arity = 1)
        public int filteringFlags = 0;

        @Parameter(names = {"--min-tlen"}, description = "Query for alignments with a template length greater than"
                + " the minimun", arity = 1)
        public int minTLen = 0;

        @Parameter(names = {"--max-tlen"}, description = "Query for alignments with a template length less than the"
                + " maximum\"", arity = 1)
        public int maxTLen = Integer.MAX_VALUE;

        @Parameter(names = {"--min-alen"}, description = "Query for alignments with an alignment length greater than"
                + " the minimun", arity = 1)
        public int minALen = 0;

        @Parameter(names = {"--max-alen"}, description = "Query for alignments with an alignment length less than the"
                + " maximum\"", arity = 1)
        public int maxALen = Integer.MAX_VALUE;
    }
}
