package org.opencb.hpg.bigdata.app.cli;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.metadata.Sample;
import org.opencb.biodata.tools.variant.metadata.VariantMetadataManager;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.utils.FileUtils;
import org.opencb.hpg.bigdata.app.cli.options.VariantCommandOptions;
import org.opencb.hpg.bigdata.core.lib.ParentDataset;
import org.opencb.hpg.bigdata.core.lib.VariantDataset;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.file.Paths.get;

/**
 * Created by jtarraga on 12/09/16.
 */
public class CliUtils {

    public static final Pattern OPERATION_PATTERN = Pattern.compile("([^=<>~!]+)(.*)$");
    public static String getOutputFilename(String input, String output, String to) throws IOException {
        String res = output;
        if (!res.isEmpty()) {
            Path parent = Paths.get(res).toAbsolutePath().getParent();
            if (parent != null) { // null if output is a file in the current directory
                FileUtils.checkDirectory(parent, true); // Throws exception, if does not exist
            }
        } else {
            res = input + "." + to;
        }
        return res;
    }

    @Deprecated
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

    public static void saveDatasetAsOneFile(ParentDataset ds, String format, String filename, Logger logger) {
        String tmpDir = filename + ".tmp";

        if ("json".equals(format)) {
            ds.coalesce(1).write().format("json").save(tmpDir);
        } else if ("parquet".equals(format)) {
            ds.coalesce(1).write().format("parquet").save(tmpDir);
        } else {
            ds.coalesce(1).write().format("com.databricks.spark.avro").save(tmpDir);
            format = "avro";
        }

        File dir = new File(tmpDir);
        if (!dir.isDirectory()) {
            // error management
            System.err.println("Error: a directory was expected but " + tmpDir);
            return;
        }

        // list out all the file name and filter by the extension
        Boolean found = false;
        String[] list = dir.list();
        for (String name: list) {
            if (name.startsWith("part-r-") && name.endsWith(format)) {
                new File(tmpDir + "/" + name).renameTo(new File(filename));
                found = true;
                break;
            }
        }
        if (!found) {
            // error management
            logger.error("Error: pattern 'part-r-*" + format + "' was not found");
            return;
        }
        dir.delete();
    }

    @Deprecated
    public static void addVariantFilters(VariantCommandOptions variantCommandOptions,
                                         VariantDataset vd) throws IOException {

        VariantMetadataManager metadataManager = new VariantMetadataManager();
        metadataManager.load(Paths.get(variantCommandOptions.queryVariantCommandOptions.input + ".meta.json"));

        // query for ID (list and file)
        List<String> list = null;
        if (StringUtils.isNotEmpty(variantCommandOptions.queryVariantCommandOptions.ids)) {
            list = Arrays.asList(StringUtils.split(variantCommandOptions.queryVariantCommandOptions.ids, ","));
        }
        String idFilename = variantCommandOptions.queryVariantCommandOptions.idFilename;
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
        if (StringUtils.isNotEmpty(variantCommandOptions.queryVariantCommandOptions.types)) {
            vd.typeFilter(Arrays.asList(
                    StringUtils.split(variantCommandOptions.queryVariantCommandOptions.types, ",")));
        }

        // query for biotype
        if (StringUtils.isNotEmpty(variantCommandOptions.queryVariantCommandOptions.biotypes)) {
            vd.annotationFilter("biotype", Arrays.asList(
                    StringUtils.split(variantCommandOptions.queryVariantCommandOptions.biotypes, ",")));
        }

        // query for study
        if (StringUtils.isNotEmpty(variantCommandOptions.queryVariantCommandOptions.studies)) {
            vd.studyFilter("studyId", Arrays.asList(
                    StringUtils.split(variantCommandOptions.queryVariantCommandOptions.studies, ",")));
        }

        // query for maf (study:cohort)
        if (StringUtils.isNotEmpty(variantCommandOptions.queryVariantCommandOptions.maf)) {
            vd.studyFilter("stats.maf", variantCommandOptions.queryVariantCommandOptions.maf);
        }

        // query for mgf (study:cohort)
        if (StringUtils.isNotEmpty(variantCommandOptions.queryVariantCommandOptions.mgf)) {
            vd.studyFilter("stats.mgf", variantCommandOptions.queryVariantCommandOptions.mgf);
        }

        // query for number of missing alleles (study:cohort)
        if (StringUtils.isNotEmpty(variantCommandOptions.queryVariantCommandOptions.missingAlleles)) {
            vd.studyFilter("stats.missingAlleles", variantCommandOptions.queryVariantCommandOptions.missingAlleles);
        }

        // query for number of missing genotypes (study:cohort)
        if (StringUtils.isNotEmpty(variantCommandOptions.queryVariantCommandOptions.missingGenotypes)) {
            vd.studyFilter("stats.missingGenotypes", variantCommandOptions.queryVariantCommandOptions.missingGenotypes);
        }

        // query for region (list and file)
        List<Region> regions = CliUtils.getRegionList(variantCommandOptions.queryVariantCommandOptions.regions,
                variantCommandOptions.queryVariantCommandOptions.regionFilename);
        if (regions != null && regions.size() > 0) {
            vd.regionFilter(regions);
        }

        // query for sample genotypes and/or sample filters
        StringBuilder sampleGenotypes = new StringBuilder();
        if (StringUtils.isNotEmpty(variantCommandOptions.queryVariantCommandOptions.sampleGenotypes)) {
            sampleGenotypes.append(variantCommandOptions.queryVariantCommandOptions.sampleGenotypes);
        }
        String sampleFilters = variantCommandOptions.queryVariantCommandOptions.sampleFilters;
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
        annotationFilterNotEmpty("consequenceTypes.sequenceOntologyTerms",
                variantCommandOptions.queryVariantCommandOptions.consequenceTypes, vd);

        // query for consequence type (gene names)
        annotationFilterNotEmpty("consequenceTypes.geneName",
                variantCommandOptions.queryVariantCommandOptions.genes, vd);

        // query for clinvar (accession)
        annotationFilterNotEmpty("variantTraitAssociation.clinvar.accession",
                variantCommandOptions.queryVariantCommandOptions.clinvar, vd);

        // query for cosmic (mutation ID)
        annotationFilterNotEmpty("variantTraitAssociation.cosmic.mutationId",
                variantCommandOptions.queryVariantCommandOptions.cosmic, vd);

        // query for conservation (phastCons, phylop, gerp)
        annotationFilterNotEmpty("conservation",
                variantCommandOptions.queryVariantCommandOptions.conservScores, vd);

        // query for protein substitution scores (polyphen, sift)
        annotationFilterNotEmpty("consequenceTypes.proteinVariantAnnotation.substitutionScores",
                variantCommandOptions.queryVariantCommandOptions.substScores, vd);

        // query for alternate population frequency (study:population)
        annotationFilterNotEmpty("populationFrequencies.altAlleleFreq",
                variantCommandOptions.queryVariantCommandOptions.pf, vd);

        // query for population minor allele frequency (study:population)
        annotationFilterNotEmpty("populationFrequencies.refAlleleFreq",
                variantCommandOptions.queryVariantCommandOptions.pmaf, vd);
    }

    @Deprecated
    private static void annotationFilterNotEmpty(String key, String value, VariantDataset vd) {
        if (StringUtils.isNotEmpty(value)) {
            vd.annotationFilter(key, value);
        }
    }

    public static VariantCommandOptions createVariantCommandOptions(
            String id, String ancestralAllele, String displayConsequenceType, String xrefs, String hgvs,
            String consequenceTypes, String consequenceSoAccession, String consequenceSoName,
            String populationFrequencies, String conservation, String variantTraitAssociation) {

        LocalCliOptionsParser parser = new LocalCliOptionsParser();
        parser.getVariantCommandOptions().queryVariantCommandOptions.ids = id;
        parser.getVariantCommandOptions().queryVariantCommandOptions.conservScores = conservation;
        parser.getVariantCommandOptions().queryVariantCommandOptions.consequenceTypes = consequenceTypes;
        parser.getVariantCommandOptions().queryVariantCommandOptions.pf = populationFrequencies;

        return parser.getVariantCommandOptions();
    }


    /**
     * Update the sample genotype query string by replacing the sample name by
     * its sample order, e.g.: from sample2:1|0,1|1 to 32:1|0,1|1.
     *
     * @param sampleGenotype     Sample genotype query string
     * @param samples            Sample list in the right order (to get the sample index)
     * @return                   Updated sample genotype query string
     */
    @Deprecated
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
            // sanity check
            if (!found) {
                // error
                System.err.format("Error: sample %s not found in dataset.\n", splits[0]);
            }
        }
        System.out.println(sampleGenotype + " -> " + newSampleGenotype.toString());
        return newSampleGenotype.toString();
    }
}
