package org.opencb.hpg.bigdata.analysis.variant;

import com.beust.jcommander.Parameter;

public class VariantFilterParameters {
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

    @Parameter(names = {"--sample-filter"}, description = "Query for sample filter, i.e.: individual attributes (family, father,"
            + " mother, sex and phenotype) and user-defined attributes from pedigree information,"
            + "  e.g.: \"individual.sex=MALE;Eyes=Blue\"", arity = 1)
    public String sampleFilters;
}
