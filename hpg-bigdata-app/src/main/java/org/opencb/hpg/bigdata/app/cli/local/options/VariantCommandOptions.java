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
@Parameters(commandNames = {"variant"}, commandDescription = "Implements different tools for working with gVCF/VCF files")
public class VariantCommandOptions {
    /*
     * Variant (VCF) CLI options
     */
    public ConvertVariantCommandOptions convertVariantCommandOptions;
    public AnnotateVariantCommandOptions annotateVariantCommandOptions;
    public ViewVariantCommandOptions viewVariantCommandOptions;
    public QueryVariantCommandOptions queryVariantCommandOptions;
    public MetadataVariantCommandOptions metadataVariantCommandOptions;
    public RvTestsVariantCommandOptions rvtestsVariantCommandOptions;

    public LocalCliOptionsParser.CommonCommandOptions commonCommandOptions;
    public JCommander jCommander;

    public VariantCommandOptions(LocalCliOptionsParser.CommonCommandOptions commonCommandOptions,
                                 JCommander jCommander) {
        this.commonCommandOptions = commonCommandOptions;
        this.jCommander = jCommander;

        this.convertVariantCommandOptions = new ConvertVariantCommandOptions();
        this.annotateVariantCommandOptions = new AnnotateVariantCommandOptions();
        this.viewVariantCommandOptions = new ViewVariantCommandOptions();
        this.queryVariantCommandOptions = new QueryVariantCommandOptions();
        this.metadataVariantCommandOptions = new MetadataVariantCommandOptions();
        this.rvtestsVariantCommandOptions = new RvTestsVariantCommandOptions();
    }

    @Parameters(commandNames = {"convert"}, commandDescription = "Convert gVCF/VCF files to different big data"
            + " formats such as Avro and Parquet using OpenCB models")
    public class ConvertVariantCommandOptions {

        @ParametersDelegate
        public LocalCliOptionsParser.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"}, description = "Input file name",
                required = true, arity = 1)
        public String input;

        @Parameter(names = {"--to"}, description = "Destination format, accepted values: avro, parquet", arity = 1)
        public String to = "avro";

        @Parameter(names = {"--from"}, description = "Input file format, accepted values: vcf, avro", arity = 1)
        public String from = "vcf";

        @Parameter(names = {"-o", "--output"}, description = "Output file name", required = true, arity = 1)
        public String output = null;

//        @Parameter(names = {"-O"}, description = "Use the standard output.", required = false, arity = 0)
//        public boolean stdOutput;

//        @Parameter(names = {"--from"}, description = "Accepted values: vcf, avro", required = false)
//        public String from;

//        @Parameter(names = {"-d", "--data-model"}, description = "Only for 'to-json' and 'to-avro' options."
// 'to-protobuf' is only available with opencb data models. Values: opencb, ga4gh", required = false, arity = 1)
//        public String dataModel = "opencb";

//        @Parameter(names = {"-x", "--compression"}, description = "Available options for Avro are: : snappy, deflate,"
// bzip2, xz. " +
//                "For JSON and ProtoBuf only 'gzip' is available. It compression is null,  it will be inferred "
// compression from file extensions: .gz, .sz, ...", required = false, arity = 1)
//        public String compression = "deflate";

        @Parameter(names = {"-x", "--compression"}, description = "Compression method, acepted values: : snappy,"
                + " gzip, bzip2, xz", arity = 1)
        public String compression = "gzip";

        @Parameter(names = {"--only-with-id"}, description = "Variants with IDs (variants with missing IDs will be"
                + " discarded)")
        public boolean validId = false;

//        @Parameter(names = {"--min-quality"}, description = "Minimum quality", arity = 1)
//        public int minQuality = 20;

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

        @Parameter(names = {"--species"}, description = "Species name", arity = 1)
        public String species = "unknown";

        @Parameter(names = {"--assembly"}, description = "Assembly name", arity = 1)
        public String assembly = "unknown";

        @Parameter(names = {"--dataset"}, description = "Dataset name", arity = 1)
        public String dataset = "noname";

        @Parameter(names = {"-t", "--num-threads"}, description = "Number of threads to use, usually this must be"
                + " less than the number of cores")
        public int numThreads = 1;

//        @Parameter(names = {"--skip-normalization"}, description = "Whether to skip variant normalization",
// required = false)
//        public boolean skipNormalization;

//        @DynamicParameter(names = {"-D"}, hidden = true)
//        public Map<String, String> options = new HashMap<>();
    }


    @Parameters(commandNames = {"annotate"}, commandDescription = "Convert gVCF/VCF files to different big data"
            + " formats such as Avro and Parquet using GA4GH models")
    public class AnnotateVariantCommandOptions {

        @ParametersDelegate
        public LocalCliOptionsParser.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"}, description = "0000 Input file name, usually a gVCF/VCF but it can be"
                + " an Avro file when converting to Parquet.",
                required = true, arity = 1)
        public String input;

        @Parameter(names = {"-o", "--output"}, description = "Input file name, usually a gVCF/VCF but it can be an"
                + " Avro file when converting to Parquet.",
                required = true, arity = 1)
        public String ouput;
    }

    @Parameters(commandNames = {"view"}, commandDescription = "Display variant Avro/Parquet files")
    public class ViewVariantCommandOptions {

        @ParametersDelegate
        public LocalCliOptionsParser.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"}, description = "Input file name (Avro/Parquet format)",
                required = true, arity = 1)
        public String input;

        @Parameter(names = {"--head"}, description = "Output the first variants of the file",
                arity = 1)
        public int head;

        @Parameter(names = {"--schema"}, description = "Display the Avro schema")
        public boolean schema = false;

        @Parameter(names = {"--exclude-samples"}, description = "Sample data are not displayed")
        public boolean excludeSamples = false;

        @Parameter(names = {"--exclude-annotations"}, description = "Annotations are not displayed")
        public boolean excludeAnnotations = false;

        @Parameter(names = {"--vcf"}, description = "Variants are displayed in VCF format")
        public boolean vcf = false;
    }

    @Parameters(commandNames = {"query"}, commandDescription = "Execute queries against the input file (Avro or"
            + " Parquet) saving the results in the output file")
    public class QueryVariantCommandOptions {

        @ParametersDelegate
        public LocalCliOptionsParser.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"}, description = "Input file name (in Avro or Parquet format).",
                required = true, arity = 1)
        public String input;

        @Parameter(names = {"-o", "--output"}, description = "Output file name. Its extension .json, .parquet and"
                + " .avro, indicates the output format.", arity = 1)
        public String output;

        @Parameter(names = {"--id"}, description = "Query for ID; comma separated list of IDs, e.g.:"
                + " \"rs312411,rs421225\"", arity = 1)
        public String ids;

        @Parameter(names = {"--id-file"}, description = "Query for ID that are stored in a file, one ID per line,"
                + " e.g.: rs312411", arity = 1)
        public String idFilename;

        @Parameter(names = {"--type"}, description = "Query for type; comma separated list of IDs, e.g.:"
                + " \"INDEL,SNP,SNV\"", arity = 1)
        public String types;

        @Parameter(names = {"--s", "--study"}, description = "Query for study; comma separated list of study names",
                arity = 1)
        public String studies;

        @Parameter(names = {"--biotype"}, description = "Query for biotype; comma separated list of biotype names,"
                + " e.g.: protein_coding, pseudogene", arity = 1)
        public String biotypes;

        @Parameter(names = {"-r", "--region"}, description = "Query for region; comma separated list of regions,"
                + " e.g.: 1:300000-400000000,15:343453463-8787665654", arity = 1)
        public String regions;

        @Parameter(names = {"--region-file"}, description = "Query for regions that are stored in a file, one region"
                + " per line,  e.g.: 1:6700000-560000000", arity = 1)
        public String regionFilename;

        @Parameter(names = {"--maf"}, description = "Query for the Minor Allele Frequency of a given study and"
                + " cohort. Use the following format enclosed with double quotes: \"study_name::cohort_name"
                + "[<|>|<=|>=|==|!=]value\", e.g.: \"1000g::all>0.4\"", arity = 1)
        public String maf;

        @Parameter(names = {"--mgf"}, description = "Query for the Minor Genotype Frequency of a given study and"
                + " cohort. Use the following format enclosed with double quotes: \"study_name::cohort_name"
                + "[<|>|<=|>=|==|!=]value\", e.g.: \"1000g::all>0.18198\"", arity = 1)
        public String mgf;

        @Parameter(names = {"--missing-allele"}, description = "Query for the number of missing alleles of a given"
                + " study and cohort. Use the following format enclosed with double quotes: \"study_name::cohort_name"
                + "[<|>|<=|>=|==|!=]value\", e.g.: \"1000g::all==5\"", arity = 1)
        public String missingAlleles;

        @Parameter(names = {"--missing-genotype"}, description = "Query for the number of missing genotypes of a"
                + " given study and cohort. Use the following format enclosed with double quotes: \"study_name::"
                + "cohort_name[<|>|<=|>=|==|!=]value\", e.g.: \"1000g::all!=0\"", arity = 1)
        public String missingGenotypes;

        @Parameter(names = {"--ct", "--consequence-type"}, description = "Query for Sequence Ontology term names or"
                + " accession codes; comma separated (use double quotes if you provide term names), e.g.:"
                + " \"transgenic insertion,SO:32234,SO:00124\"", arity = 1)
        public String consequenceTypes;

        @Parameter(names = {"--gene"}, description = "Query for gene; comma separated list of gene names, e.g.:"
                + " \"BIN3,ZNF517\"", arity = 1)
        public String genes;

        @Parameter(names = {"--clinvar"}, description = "Query for clinvar (accession); comma separated list of"
                + " accessions", arity = 1)
        public String clinvar;

        @Parameter(names = {"--cosmic"}, description = "Query for cosmic (mutation ID); comma separated list of"
                + " mutations IDs", arity = 1)
        public String cosmic;

//        @Parameter(names = {"--gwas"}, description = "Query for gwas (traits); comma separated list of traits",
// arity = 1)
//        public String gwas;

        @Parameter(names = {"--conservation"}, description = "Query for conservation scores (phastCons, phylop, gerp);"
                + "comma separated list of scores and enclosed with double quotes, e.g.: \"phylop<0.3,phastCons<0.1\"",
                arity = 1)
        public String conservScores;

        @Parameter(names = {"--ps", "--protein-substitution"}, description = "Query for protein substitution scores"
                + " (polyphen, sift); comma separated list of scores and enclosed with double quotes, e.g.:"
                + "\"polyphen>0.3,sift>0.6\"", arity = 1)
        public String substScores;

        @Parameter(names = {"--pf", "--population-frequency"}, description = "Query for alternate population"
                + " frequency of a given study. Use the following format enclosed with double quotes:"
                + " \"study_name::population_name[<|>|<=|>=|==|!=]frequency_value\", e.g.: \"1000g::CEU<0.4\"",
                arity = 1)
        public String pf;

        @Parameter(names = {"--pmaf", "--population-maf"}, description = "Query for population minor allele frequency"
                + " of a given study. Use the following the format enclosed with double quotes: \"study_name::"
                + "population_name[<|>|<=|>=|==|!=]frequency_value\", e.g.: \"1000g::PJL<=0.25\"", arity = 1)
        public String pmaf;

        @Parameter(names = {"--sample-genotype"}, description = "Query for sample genotypes. Use the following the"
                + " format enclosed with double quotes: \"sample_name1:genotype1,genotype;sample_name2:genotype1\","
                + " e.g.: \"HG00112:0/0;HG23412:1/0,1/1\"", arity = 1)
        public String sampleGenotypes;

        @Parameter(names = {"--group-by"}, description = "Display the number of output records grouped by 'gene' or"
                + " 'consequence_type'", arity = 1)
        public String groupBy;

        @Parameter(names = {"--limit"}, description = "Display the first output records", arity = 1)
        public int limit = 0;

        @Parameter(names = {"--count"}, description = "Display the number of output records")
        public boolean count = false;
    }

    @Parameters(commandNames = {"metadata"}, commandDescription = "Manage metadata information")
    public class MetadataVariantCommandOptions {

        @ParametersDelegate
        public LocalCliOptionsParser.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"}, description = "Input file name (Avro/Parquet format).",
                required = true, arity = 1)
        public String input;

        @Parameter(names = {"--dataset"}, description = "Target dataset.",
                arity = 1)
        public String datasetId = null;

        @Parameter(names = {"--load-pedigree"}, description = "Pedigree file name (in PED format) to load in the"
                + " target dataset. A pedigree file contains information about family relationships, phenotype,"
                + " gendre,... (extended PED format).", arity = 1)
        public String loadPedFilename = null;

        @Parameter(names = {"--save-pedigree"}, description = "File name where to save the pedigree information"
                + " (in PED format) of the target dataset. A pedigree file contains information about family"
                + " relationships, phenotype, gendre,... (extended PED format).", arity = 1)
        public String savePedFilename = null;

        @Parameter(names = {"--create-cohort"}, description = "Create new cohort for the target dataset. Expected"
                + " value format is: cohort_name::sample_name1,sample_name2,... or dataset_name::cohort_name::"
                + "sample_name_file\"")
        public String createCohort = null;

        @Parameter(names = {"--summary"}, description = "Output metadata summary.")
        public boolean summary = false;
    }

    @Parameters(commandNames = {"rvtests"}, commandDescription = "Execute the 'rvtests' program.")
    public class RvTestsVariantCommandOptions {

        @ParametersDelegate
        public LocalCliOptionsParser.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"}, description = "Input file name (in Avro/Parquet file format).",
                required = true, arity = 1)
        public String inFilename;

        @Parameter(names = {"-m", "--metadata"}, description = "Input metadata file name.",
                required = true, arity = 1)
        public String metaFilename;

        @Parameter(names = {"-o", "--output"}, description = "Output directory name to save the rvtests results.",
                required = true, arity = 1)
        public String outDirname;

        @Parameter(names = {"--dataset"}, description = "Target dataset.",
                arity = 1)
        public String datasetId = null;

        @Parameter(names = {"-c", "--config"}, description = "Configuration file name containing the rvtests parameters.",
                required = true, arity = 1)
        public String confFilename;
    }
}
