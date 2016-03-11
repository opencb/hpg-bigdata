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

import com.beust.jcommander.ParameterException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.ga4gh.models.Variant;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.tools.variant.simulator.VariantSimulatorConfiguration;
import org.opencb.hpg.bigdata.app.cli.CommandExecutor;
import org.opencb.hpg.bigdata.tools.io.parquet.ParquetMR;
import org.opencb.hpg.bigdata.tools.variant.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Created by imedina on 25/06/15.
 */
public class VariantCommandExecutor extends CommandExecutor {

    private CliOptionsParser.VariantCommandOptions variantCommandOptions;

    public VariantCommandExecutor(CliOptionsParser.VariantCommandOptions variantCommandOptions) {
        //      super(fastqCommandOptions.logLevel, fastqCommandOptions.verbose, fastqCommandOptions.conf);
        this.variantCommandOptions = variantCommandOptions;
    }

    @Override
    public void execute() throws Exception {
        String subCommandString = variantCommandOptions.getParsedSubCommand();
        switch (subCommandString) {
            case "convert":
                init(variantCommandOptions.convertVariantCommandOptions.commonOptions.logLevel,
                        variantCommandOptions.convertVariantCommandOptions.commonOptions.verbose,
                        variantCommandOptions.convertVariantCommandOptions.commonOptions.conf);
                convert();
                break;
            case "index":
                init(variantCommandOptions.indexVariantCommandOptions.commonOptions.logLevel,
                        variantCommandOptions.indexVariantCommandOptions.commonOptions.verbose,
                        variantCommandOptions.indexVariantCommandOptions.commonOptions.conf);
                index();
                break;
            case "simulate":
                init(variantCommandOptions.simulateVariantCommandOptions.commonOptions.logLevel,
                        variantCommandOptions.simulateVariantCommandOptions.commonOptions.verbose,
                        variantCommandOptions.simulateVariantCommandOptions.commonOptions.conf);
                simulate();
                break;
            case "sort":
                init(variantCommandOptions.simulateVariantCommandOptions.commonOptions.logLevel,
                        variantCommandOptions.simulateVariantCommandOptions.commonOptions.verbose,
                        variantCommandOptions.simulateVariantCommandOptions.commonOptions.conf);
                sort();
                break;
            case "simulatorinput":
                init(variantCommandOptions.simulatorInputVariantCommandOptions.commonOptions.logLevel,
                        variantCommandOptions.simulatorInputVariantCommandOptions.commonOptions.verbose,
                        variantCommandOptions.simulatorInputVariantCommandOptions.commonOptions.conf);
                simulatorinput();
                break;
            case "merge":
                init(variantCommandOptions.mergeVariantCommandOptions.commonOptions.logLevel,
                        variantCommandOptions.mergeVariantCommandOptions.commonOptions.verbose,
                        variantCommandOptions.mergeVariantCommandOptions.commonOptions.conf);
                merge();
                break;
            default:
                break;
        }
    }

    //    private void index() throws Exception {
    //        String input = variantCommandOptions.indexVariantCommandOptions.input;
    //        String db = variantCommandOptions.indexVariantCommandOptions.database;
    //        boolean nonVar = variantCommandOptions.indexVariantCommandOptions.includeNonVariants;
    //        boolean expand = variantCommandOptions.indexVariantCommandOptions.expand;
    //
    //        URI server = null;
    //        // new URI("//who1:60000/VariantExpanded");
    //        if (StringUtils.isNotBlank(db)) {
    //            server = new URI(db);
    //        }
    //        Variant2HbaseMR.Builder builder = new Variant2HbaseMR.Builder(input, server);
    //        builder.setExpand(expand);
    //        builder.setNonVar(nonVar);
    //        Job job = builder.build(true);
    //
    //        boolean fine = job.waitForCompletion(true);
    //        if (!fine) {
    //            throw new IllegalStateException("Variant 2 HBase failed!");
    //        }
    //    }

    private void convert() throws Exception {
        String input = variantCommandOptions.convertVariantCommandOptions.input;
        String output = variantCommandOptions.convertVariantCommandOptions.output;
        String compression = variantCommandOptions.convertVariantCommandOptions.compression;

        if (output == null) {
            output = input;
        }

        // clean paths
        //        String in = PathUtils.clean(input);
        //        String out = PathUtils.clean(output);

        if (variantCommandOptions.convertVariantCommandOptions.toParquet) {
            logger.info("Transform {} to parquet", input);

            new ParquetMR(Variant.getClassSchema()).run(input, output, compression);
            //            if (PathUtils.isHdfs(input)) {
            //                new ParquetMR(Variant.getClassSchema()).run(input, output, compression);
            //            } else {
            //                new ParquetConverter<Variant>(Variant.getClassSchema()).toParquet(new FileInputStream(input), output);
            //            }

        } else {
            Vcf2AvroMR.run(input, output, compression);
        }
    }

    private void index() throws Exception {
        String input = variantCommandOptions.indexVariantCommandOptions.input;
        String type = variantCommandOptions.indexVariantCommandOptions.type;
        String dataBase = variantCommandOptions.indexVariantCommandOptions.database;
        String credentials = variantCommandOptions.indexVariantCommandOptions.credentials;
        String hdfsPath = variantCommandOptions.indexVariantCommandOptions.hdfsPath;
        String loadType = variantCommandOptions.indexVariantCommandOptions.loadtype;
        String[] args = {input, dataBase, credentials, hdfsPath, loadType};

        if (type.equalsIgnoreCase("gff") || type.equalsIgnoreCase("bed")) {
            new LoadBEDAndGFF2HBase().run(args);
        } else if (type.equalsIgnoreCase("vcf")) {
            new BenchMarkTabix().run(args);
            //  new DataSetGeneratorMR().run(args);
        }
    }

    private void simulatorinput() throws Exception {
        String input = variantCommandOptions.simulatorInputVariantCommandOptions.input;
        String output = variantCommandOptions.simulatorInputVariantCommandOptions.output;
        // String chrProb = variantCommandOptions.simulatorInputVariantCommandOptions.chromosomeprobability;
        int numVariants  = variantCommandOptions.simulatorInputVariantCommandOptions.numVariants;
        String[] args = {input, output, Integer.toString(numVariants)};
        new DataSetGeneratorMR().run(args);

    }

    private void simulate() throws IOException {

        // TODO check if parent folder exist in HDFS

        String output = variantCommandOptions.simulateVariantCommandOptions.output;
        Configuration config = new Configuration();
        FileSystem hdfs = FileSystem.get(config);
        Path path = new Path(output);

//        boolean isExists = hdfs.exists(path);
        //        if (isExists) {
        //        }

        int numVariants = variantCommandOptions.simulateVariantCommandOptions.numVariants;
        if (numVariants <= 0) {
            logger.warn("Number of variants is <=0, it is set to 1000000");
            numVariants = 1000000;
        }

        int numSamples = variantCommandOptions.simulateVariantCommandOptions.numSamples;
        if (numSamples <= 0) {
            logger.warn("Number of samples is <=0, it is set to 20");
            numSamples = 20;
        }

        List<Region> regions = new ArrayList<>();
        if (variantCommandOptions.simulateVariantCommandOptions.regions != null) {
            regions = Region.parseRegions(variantCommandOptions.simulateVariantCommandOptions.regions);
        }

        //Written by Nacho
        /*Map<String, Double> genotypeFrequencies = new HashMap<>();
        if (variantCommandOptions.simulateVariantCommandOptions.genotypeFreqs != null) {
            String[] genotypeFreqsArray = variantCommandOptions.simulateVariantCommandOptions.genotypeFreqs.split(",");
            if (genotypeFreqsArray.length == 4) {
                genotypeFrequencies.put("0/0", Double.parseDouble(genotypeFreqsArray[0]));
                genotypeFrequencies.put("0/1", Double.parseDouble(genotypeFreqsArray[1]));
                genotypeFrequencies.put("1/1", Double.parseDouble(genotypeFreqsArray[2]));
                genotypeFrequencies.put("./.", Double.parseDouble(genotypeFreqsArray[3]));
            } else {
                throw new ParameterException("--genotype-freqs param needs 4 fields");
            }
        }*/


        String genotypeFrequencies = null;
        if (variantCommandOptions.simulateVariantCommandOptions.genotypeFreqs != null) {
            String[] genotypeFreqsArray = variantCommandOptions.simulateVariantCommandOptions.genotypeFreqs.split(",");
            if (genotypeFreqsArray.length == 4) {
                genotypeFrequencies = variantCommandOptions.simulateVariantCommandOptions.genotypeFreqs;
            } else {
                throw new ParameterException("--genotype-freqs param needs 4 fields");
            }
        }

        Map<String, Double> typeFrequencies = new HashMap<>();
        if (variantCommandOptions.simulateVariantCommandOptions.typeFreqs != null) {
            String[] typeFreqsArray = variantCommandOptions.simulateVariantCommandOptions.typeFreqs.split(",");
            if (typeFreqsArray.length == 3) {
                typeFrequencies.put("SNV", Double.parseDouble(typeFreqsArray[0]));
                typeFrequencies.put("INDEL", Double.parseDouble(typeFreqsArray[1]));
                typeFrequencies.put("SV", Double.parseDouble(typeFreqsArray[2]));
            } else {
                throw new ParameterException("--type-freqs param needs 3 fields");
            }
        }

        //Written by Nacho
        // Variant simulator configuration object is created with all the parsed parameters
        VariantSimulatorConfiguration variantSimulatorConfiguration = new VariantSimulatorConfiguration();
        //variantSimulatorConfiguration.setNumSamples(numSamples);
        //variantSimulatorConfiguration.setGenotypeProbabilities(genotypeFrequencies);
        //        variantSimulatorConfiguration.setTypeProbabilities(typeFrequencies);
        // if region is different from nul lwe overwrite them, otherwise the default human genome is used
        //        if (regions != null && !regions.isEmpty()) {
        //            variantSimulatorConfiguration.setRegions(regions);
        //        }


        String [] args = {output, regions.toString(), genotypeFrequencies.toString(), String.valueOf(numSamples)};
        //String [] args = {output};
        try {
            VariantSimulatorMR variantSimulatorMR = new VariantSimulatorMR(variantSimulatorConfiguration);
            variantSimulatorMR.setNumVariants(numVariants);
            variantSimulatorMR.setChunkSize(variantCommandOptions.simulateVariantCommandOptions.chunkSize);
            variantSimulatorMR.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }

        //        SimulatorConfiguration simulatorConfiguration;
        //        if (variantCommandOptions.simulateVariantCommandOptions.commonOptions.conf != null) {
        //            Path confPath = Paths.get(variantCommandOptions.simulateVariantCommandOptions.commonOptions.conf);
        //            if (confPath.toFile().exists()) {
        //                System.out.println("Conf path :: " + confPath.toFile());
        //                simulatorConfiguration = SimulatorConfiguration.load(new FileInputStream(confPath.toFile()));
        //                System.out.println("Conf path :: " + confPath.toFile());
        //            } else {
        //                throw new ParameterException("File not found");
        //            }
        //        } else {
        //            simulatorConfiguration = SimulatorConfiguration.load(new FileInputStream(new File(appHome
        //                    + "/simulator-conf.yml")));
        //        }
        //        System.out.println("Executor :: " + simulatorConfiguration.toString());
        //        List<Region> regions = simulatorConfiguration.getRegions();
        //        Map<String, Float> genProb = simulatorConfiguration.getGenotypeProbabilities();

    }

    private void sort() {
        //logger.info("Not implemented yet");
        String input = variantCommandOptions.sortVariantCommandOptions.input;
        String output = variantCommandOptions.sortVariantCommandOptions.output;

        String [] args = {input, output};
        //VariantSimulatorAvroSortMR variantSimulatorAvroSortMR = new VariantSimulatorAvroSortMR();
        VariantAvroSortCustomMR variantAvroSortCustomMR = new VariantAvroSortCustomMR();
        try {
            //variantSimulatorAvroSortMR.run(args);
            variantAvroSortCustomMR.run(args);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void merge() {
        //logger.info("Not implemented yet");
        String input = variantCommandOptions.mergeVariantCommandOptions.input;
        String output = variantCommandOptions.mergeVariantCommandOptions.output;

        String [] args = {input, output};
        VariantAvroMergeMR varaintAvroMergeMR = new VariantAvroMergeMR();
        try {
            varaintAvroMergeMR.run(args);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
