package org.opencb.hpg.bigdata.app.cli.local;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.core.Region;
import org.opencb.commons.utils.FileUtils;
import org.opencb.hpg.bigdata.app.cli.local.options.VariantCommandOptions;
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

import static java.nio.file.Paths.get;

/**
 * Created by jtarraga on 12/09/16.
 */
public class CliUtils {

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

    public static void addVariantFilters(VariantCommandOptions variantCommandOptions,
                                         VariantDataset vd) throws IOException {
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

        // query for sample genotypes
        if (StringUtils.isNotEmpty(variantCommandOptions.queryVariantCommandOptions.sampleGenotypes)) {
            vd.sampleFilter("GT", variantCommandOptions.queryVariantCommandOptions.sampleGenotypes);
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
}
