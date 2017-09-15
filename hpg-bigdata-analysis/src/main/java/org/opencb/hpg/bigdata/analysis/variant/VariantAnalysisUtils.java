package org.opencb.hpg.bigdata.analysis.variant;

import htsjdk.variant.variantcontext.writer.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.opencb.biodata.formats.pedigree.PedigreeManager;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.core.pedigree.Pedigree;
import org.opencb.biodata.models.metadata.Sample;
import org.opencb.biodata.models.variant.metadata.VariantStudyMetadata;
import org.opencb.biodata.tools.variant.converters.VCFExporter;
import org.opencb.biodata.tools.variant.metadata.VariantMetadataManager;
import org.opencb.commons.datastore.core.Query;
import org.opencb.hpg.bigdata.core.lib.SparkConfCreator;
import org.opencb.hpg.bigdata.core.lib.VariantDataset;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.file.Paths.get;

public class VariantAnalysisUtils {
    public static final Pattern OPERATION_PATTERN = Pattern.compile("([^=<>~!]+)(.*)$");

    public static void exportVCF(String inputAvroFilename, String metaFilename,
                                 FilterParameters filterParams, String vcfFilename) {
        // Generate VCF file by calling VCF exporter from query and query options
        VariantMetadataManager manager = new VariantMetadataManager();
        try {
            manager.load(Paths.get(metaFilename));

            SparkConf sparkConf = SparkConfCreator.getConf("tool plink", "local", 1, true);
            SparkSession sparkSession = new SparkSession(new SparkContext(sparkConf));

            VariantDataset vd = new VariantDataset(sparkSession);
            vd.load(inputAvroFilename);
            vd.createOrReplaceTempView("vcf");

            // Add filters to variant dataset
            if (filterParams != null) {
                addVariantFilters(filterParams, vd);
            }

            // out filename
            VariantStudyMetadata studyMetadata = manager.getVariantMetadata().getStudies().get(0);
            VCFExporter vcfExporter = new VCFExporter(studyMetadata);
            vcfExporter.open(Options.ALLOW_MISSING_FIELDS_IN_HEADER, Paths.get(vcfFilename));

            vcfExporter.export(vd.iterator());

            // close everything
            vcfExporter.close();
            sparkSession.stop();
        } catch (Exception e) {
            System.out.println("Error retrieving variants from Avro to VCF file: " + e.getMessage());
        }
    }

    public static void addVariantFilters(FilterParameters filterParams, VariantDataset vd) throws IOException {
        VariantMetadataManager metadataManager = new VariantMetadataManager();
        //metadataManager.load(Paths.get(variantCommandOptions.queryVariantCommandOptions.input + ".meta.json"));

        // query for ID (list and file)
        List<String> list = null;
        if (StringUtils.isNotEmpty(filterParams.ids)) {
            list = Arrays.asList(StringUtils.split(filterParams.ids, ","));
        }
        String idFilename = filterParams.idFilename;
        if (StringUtils.isNotEmpty(idFilename) && new File(idFilename).exists()) {
            if (list == null) {
                list = Files.readAllLines(get(idFilename));
            } else {
                list.addAll(Files.readAllLines(get(idFilename)));
            }
        }
        if (list != null) {
            vd.idFilter(list, false);
        }

        // query for type
        if (StringUtils.isNotEmpty(filterParams.types)) {
            vd.typeFilter(Arrays.asList(
                    StringUtils.split(filterParams.types, ",")));
        }

        // query for biotype
        if (StringUtils.isNotEmpty(filterParams.biotypes)) {
            vd.annotationFilter("biotype", Arrays.asList(
                    StringUtils.split(filterParams.biotypes, ",")));
        }

        // query for study
        if (StringUtils.isNotEmpty(filterParams.studies)) {
            vd.studyFilter("studyId", Arrays.asList(
                    StringUtils.split(filterParams.studies, ",")));
        }

        // query for maf (study:cohort)
        if (StringUtils.isNotEmpty(filterParams.maf)) {
            vd.studyFilter("stats.maf", filterParams.maf);
        }

        // query for mgf (study:cohort)
        if (StringUtils.isNotEmpty(filterParams.mgf)) {
            vd.studyFilter("stats.mgf", filterParams.mgf);
        }

        // query for number of missing alleles (study:cohort)
        if (StringUtils.isNotEmpty(filterParams.missingAlleles)) {
            vd.studyFilter("stats.missingAlleles", filterParams.missingAlleles);
        }

        // query for number of missing genotypes (study:cohort)
        if (StringUtils.isNotEmpty(filterParams.missingGenotypes)) {
            vd.studyFilter("stats.missingGenotypes", filterParams.missingGenotypes);
        }

        // query for region (list and file)
        List<Region> regions = getRegionList(filterParams.regions,
                filterParams.regionFilename);
        if (regions != null && regions.size() > 0) {
            vd.regionFilter(regions);
        }

        // query for sample genotypes and/or sample filters
        StringBuilder sampleGenotypes = new StringBuilder();
        if (StringUtils.isNotEmpty(filterParams.sampleGenotypes)) {
            sampleGenotypes.append(filterParams.sampleGenotypes);
        }
        String sampleFilters = filterParams.sampleFilters;
        if (StringUtils.isNotEmpty(sampleGenotypes) || StringUtils.isNotEmpty(sampleFilters)) {
            // TODO: we need the ID for dataset target
            List<Sample> samples = null;


            if (StringUtils.isNotEmpty(sampleFilters)) {
                Query sampleQuery = new Query();
//                final Pattern OPERATION_PATTERN = Pattern.compile("([^=<>~!]+.*)(<=?|>=?|!=|!?=?~|==?)([^=<>~!]+.*)$");
                String[] splits = sampleFilters.split("[;]");
                for (int i = 0; i < splits.length; i++) {
                    Matcher matcher = OPERATION_PATTERN.matcher(splits[i]);
                    if (matcher.matches()) {
                        sampleQuery.put(matcher.group(1), matcher.group(2));
                    }
                }

                samples = metadataManager.getSamples(sampleQuery,
                        metadataManager.getVariantMetadata().getStudies().get(0).getId());

                for (Sample sample : samples) {
                    if (sampleGenotypes.length() > 0) {
                        sampleGenotypes.append(";");
                    }
                    //sampleGenotypes.append(sample.getId()).append(":0|1,1|0,1|1");
                    sampleGenotypes.append(sample.getId()).append(":1|1");
                }
            }
            samples = metadataManager.getSamples(
                    metadataManager.getVariantMetadata().getStudies().get(0).getId());

            // e.g.: sample genotypes = sample1:0|0;sample2:1|0,1|1
            String[] values = sampleGenotypes.toString().split("[;]");
            StringBuilder newSampleGenotypes = new StringBuilder();
            if (values == null) {
                newSampleGenotypes.append(updateSampleGenotype(sampleGenotypes.toString(), samples));
            } else {
                newSampleGenotypes.append(updateSampleGenotype(values[0], samples));
                for (int i = 1; i < values.length; i++) {
                    newSampleGenotypes.append(";");
                    newSampleGenotypes.append(updateSampleGenotype(values[i], samples));
                }
            }
            if (!StringUtils.isEmpty(newSampleGenotypes)) {
                vd.sampleFilter("GT", newSampleGenotypes.toString());
            } else {
                System.err.format("Error: could not parse your sample genotypes %s.\n", sampleGenotypes);
            }
        }

        // query for consequence type (Sequence Ontology term names and accession codes)
        annotationFilterNotEmpty("consequenceTypes.sequenceOntologyTerms", filterParams.consequenceTypes, vd);

        // query for consequence type (gene names)
        annotationFilterNotEmpty("consequenceTypes.geneName", filterParams.genes, vd);

        // query for clinvar (accession)
        annotationFilterNotEmpty("variantTraitAssociation.clinvar.accession", filterParams.clinvar, vd);

        // query for cosmic (mutation ID)
        annotationFilterNotEmpty("variantTraitAssociation.cosmic.mutationId", filterParams.cosmic, vd);

        // query for conservation (phastCons, phylop, gerp)
        annotationFilterNotEmpty("conservation", filterParams.conservScores, vd);

        // query for protein substitution scores (polyphen, sift)
        annotationFilterNotEmpty("consequenceTypes.proteinVariantAnnotation.substitutionScores", filterParams.substScores, vd);

        // query for alternate population frequency (study:population)
        annotationFilterNotEmpty("populationFrequencies.altAlleleFreq", filterParams.pf, vd);

        // query for population minor allele frequency (study:population)
        annotationFilterNotEmpty("populationFrequencies.refAlleleFreq", filterParams.pmaf, vd);
    }

    private static void annotationFilterNotEmpty(String key, String value, VariantDataset vd) {
        if (StringUtils.isNotEmpty(value)) {
            vd.annotationFilter(key, value);
        }
    }

    public static List<Region> getRegionList(String regions, String regionFilename) throws IOException {
        List<Region> list = null;
        if (StringUtils.isNotEmpty(regions)) {
            list = Region.parseRegions(regions);
        }
        if (StringUtils.isNotEmpty(regionFilename) && new File(regionFilename).exists()) {
            if (regions == null) {
                list = new ArrayList<>();
            }
            List<String> lines = Files.readAllLines(Paths.get(regionFilename));
            for (String line : lines) {
                list.add(new Region(line));
            }
        }
        return list;
    }

    /**
     * Update the sample genotype query string by replacing the sample name by
     * its sample order, e.g.: from sample2:1|0,1|1 to 32:1|0,1|1.
     *
     * @param sampleGenotype     Sample genotype query string
     * @param samples            Sample list in the right order (to get the sample index)
     * @return                   Updated sample genotype query string
     */
    private static String updateSampleGenotype(String sampleGenotype, List<Sample> samples) {
        // e.g.: value = sample2:1|0,1|1
        StringBuilder newSampleGenotype = new StringBuilder("");
        String[] splits = sampleGenotype.split("[:]");
        if (splits == null) {
            // error
            System.err.format("Error: invalid expresion %s for sample genotypes.\n", sampleGenotype);
        } else {
            boolean found = false;
            // TODO: move this functionality to the VariantMetadataManager (from sample name to sample index)
            for (int i = 0; i < samples.size(); i++) {
                if (splits[0].equals(samples.get(i).getId())) {
                    newSampleGenotype.append(i).append(":").append(splits[1]);
                    found = true;
                    break;
                }
            }
            // Sanity check
            if (!found) {
                // error
                System.err.format("Error: sample %s not found in dataset.\n", splits[0]);
            }
        }
        System.out.println(sampleGenotype + " -> " + newSampleGenotype.toString());
        return newSampleGenotype.toString();
    }

    /**
     * Export variant metadata into pedigree file format.
     *
     * @param metaFilename  Variant metadata file name
     * @param studyId       Study ID target
     * @param pedFilename   Pedigree file name
     * @throws IOException  IO exception
     */
    public static void exportPedigree(String metaFilename, String studyId, String pedFilename) throws IOException {
        VariantMetadataManager metadataManager = new VariantMetadataManager();
        metadataManager.load(Paths.get(metaFilename));
        List<Pedigree> pedigrees = metadataManager.getPedigree(studyId);
        PedigreeManager pedigreeManager = new PedigreeManager();
        pedigreeManager.save(pedigrees, Paths.get(pedFilename));
    }
}
