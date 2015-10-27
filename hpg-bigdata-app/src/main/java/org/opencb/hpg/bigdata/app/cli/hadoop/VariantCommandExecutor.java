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

//import java.io.File;
//import java.io.FileInputStream;
//import javax.management.Query;
import java.io.IOException;
//import java.nio.file.Path;
//import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.ga4gh.models.Variant;
import org.opencb.biodata.models.core.Region;
import org.opencb.hpg.bigdata.app.cli.CommandExecutor;
import org.opencb.hpg.bigdata.tools.io.parquet.ParquetMR;
import org.opencb.hpg.bigdata.tools.variant.BenchMarkTabix;
import org.opencb.hpg.bigdata.tools.variant.DataSetGeneratorMR;
import org.opencb.hpg.bigdata.tools.variant.LoadBEDAndGFF2HBase;
import org.opencb.hpg.bigdata.tools.variant.VariantSimulatorMR;
import org.opencb.hpg.bigdata.tools.variant.Vcf2AvroMR;

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
        case "simulatorinput":
            init(variantCommandOptions.simulatorInputVariantCommandOptions.commonOptions.logLevel,
                    variantCommandOptions.simulatorInputVariantCommandOptions.commonOptions.verbose,
                    variantCommandOptions.simulatorInputVariantCommandOptions.commonOptions.conf);
            simulatorinput();
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

        List<Region> regions = Arrays.asList(Region.parseRegion("1"), Region.parseRegion("2:40000-50000"),
                Region.parseRegion("3:1000000"));

        Map<String, Float> genProb = new HashMap<>();
        genProb.put("0/0", 0.7f);
        genProb.put("0/1", 0.2f);
        genProb.put("1/1", 0.08f);
        genProb.put("./.", 0.02f);

        String input = variantCommandOptions.simulateVariantCommandOptions.input;
        String output = variantCommandOptions.simulateVariantCommandOptions.output;

        //String conf = variantCommandOptions.simulateVariantCommandOptions.conf;
        //String genProbMap = variantCommandOptions.simulateVariantCommandOptions.output;

        String [] args = {input, output, regions.toString(), genProb.toString()};
        try {
            //new VariantSimulatorMR().run(input, output, regions.toString(), genProb.toString());
            new VariantSimulatorMR().run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

}
