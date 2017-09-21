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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterDescription;
import com.beust.jcommander.ParameterException;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.metadata.Sample;
import org.opencb.biodata.tools.variant.metadata.VariantMetadataManager;
import org.opencb.commons.datastore.core.Query;
import org.opencb.hpg.bigdata.app.cli.local.options.VariantFilterOptions;
import org.opencb.hpg.bigdata.app.cli.local.options.*;
import org.opencb.hpg.bigdata.core.lib.VariantDataset;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.file.Paths.get;

/**
 * Created by imedina on 16/06/15.
 */
public class LocalCliOptionsParser {

    private final JCommander jcommander;

    private final GeneralOptions generalOptions;
    private final CommandOptions commandOptions;

    private final CommonCommandOptions commonCommandOptions;
    private final VariantFilterOptions filterOptions;

    private AdminCommandOptions adminCommandOptions;

    // NGS Sequence command and subcommmands
    private SequenceCommandOptions sequenceCommandOptions;

    // NGS Alignments command and subcommmands
    private AlignmentCommandOptions alignmentCommandOptions;

    // NGS variant command and subcommmands
    private VariantCommandOptions variantCommandOptions;

    private ToolCommandOptions toolCommandOptions;

    public LocalCliOptionsParser() {
        generalOptions = new GeneralOptions();
        jcommander = new JCommander(generalOptions);

        commandOptions = new CommandOptions();
        commonCommandOptions = new CommonCommandOptions();
        filterOptions = new VariantFilterOptions();

        adminCommandOptions = new AdminCommandOptions(commonCommandOptions, jcommander);
        jcommander.addCommand("admin", adminCommandOptions);
        JCommander adminSubCommands = jcommander.getCommands().get("admin");
        adminSubCommands.addCommand("server", adminCommandOptions.serverAdminCommandOptions);

        sequenceCommandOptions = new SequenceCommandOptions(commonCommandOptions, jcommander);
        jcommander.addCommand("sequence", sequenceCommandOptions);
        JCommander sequenceSubCommands = jcommander.getCommands().get("sequence");
        sequenceSubCommands.addCommand("convert", sequenceCommandOptions.convertSequenceCommandOptions);
        sequenceSubCommands.addCommand("stats", sequenceCommandOptions.statsSequenceCommandOptions);

        alignmentCommandOptions = new AlignmentCommandOptions(commonCommandOptions, jcommander);
        jcommander.addCommand("alignment", alignmentCommandOptions);
        JCommander alignmentSubCommands = jcommander.getCommands().get("alignment");
        alignmentSubCommands.addCommand("convert", alignmentCommandOptions.convertAlignmentCommandOptions);
        alignmentSubCommands.addCommand("view", alignmentCommandOptions.viewAlignmentCommandOptions);
        alignmentSubCommands.addCommand("sort", alignmentCommandOptions.sortAlignmentCommandOptions);
        alignmentSubCommands.addCommand("stats", alignmentCommandOptions.statsAlignmentCommandOptions);
        alignmentSubCommands.addCommand("coverage", alignmentCommandOptions.coverageAlignmentCommandOptions);
        alignmentSubCommands.addCommand("query", alignmentCommandOptions.queryAlignmentCommandOptions);

        variantCommandOptions = new VariantCommandOptions(commonCommandOptions, filterOptions, jcommander);
        jcommander.addCommand("variant", variantCommandOptions);
        JCommander variantSubCommands = jcommander.getCommands().get("variant");
        variantSubCommands.addCommand("convert", variantCommandOptions.convertVariantCommandOptions);
        variantSubCommands.addCommand("stats", variantCommandOptions.statsVariantCommandOptions);
        variantSubCommands.addCommand("annotate", variantCommandOptions.annotateVariantCommandOptions);
        variantSubCommands.addCommand("view", variantCommandOptions.viewVariantCommandOptions);
        variantSubCommands.addCommand("query", variantCommandOptions.queryVariantCommandOptions);
        variantSubCommands.addCommand("metadata", variantCommandOptions.metadataVariantCommandOptions);
        variantSubCommands.addCommand("rvtests", variantCommandOptions.rvtestsVariantCommandOptions);
        variantSubCommands.addCommand("plink", variantCommandOptions.plinkVariantCommandOptions);

        toolCommandOptions = new ToolCommandOptions(commonCommandOptions, jcommander);
        jcommander.addCommand("tool", toolCommandOptions);
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

    public boolean existSubcommands() {
        return jcommander.getCommands().get(jcommander.getParsedCommand()).getCommands().size() > 0;
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

        @Parameter(names = {"-L", "--log-level"}, description = "Set the level log, values: debug, info, warning, error, fatal", arity = 1)
        public String logLevel = "info";

        @Deprecated
        @Parameter(names = {"-v", "--verbose"}, description = "This parameter set the level of the logging", arity = 1)
        public boolean verbose;

        @Parameter(names = {"--conf"}, description = "Set the configuration file", arity = 1)
        public String conf;

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
                if (existSubcommands()) {
                    System.err.println("");
                    System.err.println("Usage:   hpg-bigdata-local.sh " + parsedCommand + " <subcommand> [options]");
                    System.err.println("");
                    System.err.println("Subcommands:");
                    printCommandUsage(jcommander.getCommands().get(getCommand()));
                    System.err.println("");
                } else {
                    // There are no subcommands
                    System.err.println("");
                    System.err.println("Usage:   hpg-bigdata-local.sh " + parsedCommand + " [options]");
                    System.err.println("");
                    System.err.println("Options:");
                    printSubCommandUsage(jcommander.getCommands().get(parsedCommand));
                    System.err.println("");
                }
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
        for (String s : jcommander.getCommands().keySet()) {
            System.err.printf("%12s  %s\n", s, jcommander.getCommandDescription(s));
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

    public static Query variantFilterOptionsParser(VariantFilterOptions variantFilterOptions) throws IOException {
        final Pattern OPERATION_PATTERN = Pattern.compile("([^=<>~!]+)(.*)$");

        VariantDataset vd = new VariantDataset(null);
        VariantMetadataManager metadataManager = new VariantMetadataManager();

        // Query for ID (list and file)
        List<String> list = null;
        if (StringUtils.isNotEmpty(variantFilterOptions.ids)) {
            list = Arrays.asList(StringUtils.split(variantFilterOptions.ids, ","));
        }
        String idFilename = variantFilterOptions.idFilename;
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

        // Query for type
        if (StringUtils.isNotEmpty(variantFilterOptions.types)) {
            vd.typeFilter(Arrays.asList(
                    StringUtils.split(variantFilterOptions.types, ",")));
        }

        // Query for biotype
        if (StringUtils.isNotEmpty(variantFilterOptions.biotypes)) {
            vd.annotationFilter("biotype", Arrays.asList(
                    StringUtils.split(variantFilterOptions.biotypes, ",")));
        }

        // Query for study
        if (StringUtils.isNotEmpty(variantFilterOptions.studies)) {
            vd.studyFilter("studyId", Arrays.asList(
                    StringUtils.split(variantFilterOptions.studies, ",")));
        }

        // Query for maf (study:cohort)
        if (StringUtils.isNotEmpty(variantFilterOptions.maf)) {
            vd.studyFilter("stats.maf", variantFilterOptions.maf);
        }

        // Query for mgf (study:cohort)
        if (StringUtils.isNotEmpty(variantFilterOptions.mgf)) {
            vd.studyFilter("stats.mgf", variantFilterOptions.mgf);
        }

        // Query for number of missing alleles (study:cohort)
        if (StringUtils.isNotEmpty(variantFilterOptions.missingAlleles)) {
            vd.studyFilter("stats.missingAlleles", variantFilterOptions.missingAlleles);
        }

        // Query for number of missing genotypes (study:cohort)
        if (StringUtils.isNotEmpty(variantFilterOptions.missingGenotypes)) {
            vd.studyFilter("stats.missingGenotypes", variantFilterOptions.missingGenotypes);
        }

        // Query for region (list and file)
        List<Region> regions = getRegionList(variantFilterOptions.regions,
                variantFilterOptions.regionFilename);
        if (regions != null && regions.size() > 0) {
            vd.regionFilter(regions);
        }

        // Query for sample genotypes and/or sample filters
        StringBuilder sampleGenotypes = new StringBuilder();
        if (StringUtils.isNotEmpty(variantFilterOptions.sampleGenotypes)) {
            sampleGenotypes.append(variantFilterOptions.sampleGenotypes);
        }
        String sampleFilters = variantFilterOptions.sampleFilters;
        if (StringUtils.isNotEmpty(sampleGenotypes) || StringUtils.isNotEmpty(sampleFilters)) {
            // TODO: we need the ID for dataset target
            List<Sample> samples = null;


            if (StringUtils.isNotEmpty(sampleFilters)) {
                Query sampleQuery = new Query();
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

            // Genotype format: sample genotypes = sample1:0|0;sample2:1|0,1|1
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

        // Query for consequence type (Sequence Ontology term names and accession codes)
        annotationFilterNotEmpty("consequenceTypes.sequenceOntologyTerms", variantFilterOptions.consequenceTypes, vd);

        // Query for consequence type (gene names)
        annotationFilterNotEmpty("consequenceTypes.geneName", variantFilterOptions.genes, vd);

        // Query for clinvar (accession)
        annotationFilterNotEmpty("variantTraitAssociation.clinvar.accession", variantFilterOptions.clinvar, vd);

        // Query for cosmic (mutation ID)
        annotationFilterNotEmpty("variantTraitAssociation.cosmic.mutationId", variantFilterOptions.cosmic, vd);

        // Query for conservation (phastCons, phylop, gerp)
        annotationFilterNotEmpty("conservation", variantFilterOptions.conservScores, vd);

        // Query for protein substitution scores (polyphen, sift)
        annotationFilterNotEmpty("consequenceTypes.proteinVariantAnnotation.substitutionScores", variantFilterOptions.substScores, vd);

        // Query for alternate population frequency (study:population)
        annotationFilterNotEmpty("populationFrequencies.altAlleleFreq", variantFilterOptions.pf, vd);

        // Query for population minor allele frequency (study:population)
        annotationFilterNotEmpty("populationFrequencies.refAlleleFreq", variantFilterOptions.pmaf, vd);

        return vd.getQuery();
    }

    private static void annotationFilterNotEmpty(String key, String value, VariantDataset vd) {
        if (StringUtils.isNotEmpty(value)) {
            vd.annotationFilter(key, value);
        }
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
        // Genotype format, e.g.: value = sample2:1|0,1|1
        StringBuilder newSampleGenotype = new StringBuilder("");
        String[] splits = sampleGenotype.split("[:]");
        if (splits == null) {
            // Error
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

    private static List<Region> getRegionList(String regions, String regionFilename) throws IOException {
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

    public ToolCommandOptions getToolCommandOptions() {
        return toolCommandOptions;
    }
}
