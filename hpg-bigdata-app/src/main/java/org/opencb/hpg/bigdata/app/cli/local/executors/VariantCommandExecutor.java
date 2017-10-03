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

package org.opencb.hpg.bigdata.app.cli.local.executors;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.opencb.biodata.formats.pedigree.PedigreeManager;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.core.pedigree.Pedigree;
import org.opencb.biodata.models.metadata.Cohort;
import org.opencb.biodata.models.metadata.SampleSetType;
import org.opencb.biodata.models.variant.avro.StudyEntry;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.biodata.tools.variant.metadata.VariantMetadataManager;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.utils.FileUtils;
import org.opencb.hpg.bigdata.analysis.variant.wrappers.PlinkWrapper;
import org.opencb.hpg.bigdata.analysis.variant.wrappers.RvTestsWrapper;
import org.opencb.hpg.bigdata.app.cli.CommandExecutor;
import org.opencb.hpg.bigdata.app.cli.local.CliUtils;
import org.opencb.hpg.bigdata.app.cli.local.LocalCliOptionsParser;
import org.opencb.hpg.bigdata.app.cli.local.options.VariantCommandOptions;
import org.opencb.hpg.bigdata.core.avro.VariantAvroAnnotator;
import org.opencb.hpg.bigdata.core.avro.VariantAvroSerializer;
import org.opencb.hpg.bigdata.core.lib.SparkConfCreator;
import org.opencb.hpg.bigdata.core.lib.VariantDataset;
import org.opencb.hpg.bigdata.core.parquet.VariantParquetConverter;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static java.nio.file.Paths.get;

//import org.opencb.hpg.bigdata.analysis.variant.analysis.RvTestsWrapper;

/**
 * Created by imedina on 25/06/15.
 */
public class VariantCommandExecutor extends CommandExecutor {

    private VariantCommandOptions variantCommandOptions;

    public VariantCommandExecutor(VariantCommandOptions variantCommandOptions) {
        super(variantCommandOptions.commonCommandOptions);
        this.variantCommandOptions = variantCommandOptions;
    }

    @Override
    public void execute() throws Exception {
        // init
        init(variantCommandOptions.commonCommandOptions.logLevel,
                variantCommandOptions.commonCommandOptions.verbose,
                variantCommandOptions.commonCommandOptions.conf);

        String subCommandString = getParsedSubCommand(variantCommandOptions.jCommander);
        switch (subCommandString) {
            case "convert":
                convert();
                break;
            case "annotate":
                annotate();
                break;
            case "view":
                view();
                break;
            case "query":
                query();
                break;
            case "metadata":
                metadata();
                break;
            case "rvtests":
                rvtests();
                break;
            case "plink":
                plink();
                break;
            default:
                logger.error("Variant subcommand '" + subCommandString + "' not valid");
                break;
        }
    }

    private void convert() throws IOException {
        // sanity check: paremeter 'to'
        String to = variantCommandOptions.convertVariantCommandOptions.to.toLowerCase();
        if (!to.equals("avro") && !to.equals("parquet")) {
            throw new IllegalArgumentException("Unknown serialization format: " + to + ". Valid values: avro, parquet");
        }
        boolean toParquet = to.equals("parquet");

        String from = variantCommandOptions.convertVariantCommandOptions.from.toLowerCase();
        if (!from.equals("vcf") && !from.equals("avro")) {
            throw new IllegalArgumentException("Unknown input format: " + from + ". Valid values: vcf, avro");
        }
        boolean fromAvro = from.equals("avro");

        // sanity check: parameter 'compression'
        String compressionCodecName = variantCommandOptions.convertVariantCommandOptions.compression.toLowerCase();
        if (!compressionCodecName.equals("gzip")
                && !compressionCodecName.equals("snappy")) {
            throw new IllegalArgumentException("Unknown compression method: " + compressionCodecName
                    + ". Valid values: gzip, snappy");
        }

        // sanity check: input file
        Path inputPath = Paths.get(variantCommandOptions.convertVariantCommandOptions.input);
        FileUtils.checkFile(inputPath);

        // sanity check: output file
        String output = CliUtils.getOutputFilename(variantCommandOptions.convertVariantCommandOptions.input,
                variantCommandOptions.convertVariantCommandOptions.output, to);

        // annotate before converting
        boolean annotate = variantCommandOptions.convertVariantCommandOptions.annotate;

        long startTime, elapsedTime;

        // convert to parquet if required
        if (toParquet) {
            // sanity check: rowGroupSize and pageSize for parquet conversion
            int rowGroupSize = variantCommandOptions.convertVariantCommandOptions.blockSize;
            if (rowGroupSize <= 0) {
                throw new IllegalArgumentException("Invalid block size: " + rowGroupSize
                        + ". It must be greater than 0");
            }
            int pageSize = variantCommandOptions.convertVariantCommandOptions.pageSize;
            if (pageSize <= 0) {
                throw new IllegalArgumentException("Invalid page size: " + pageSize
                        + ". It must be greater than 0");
            }

            // create the Parquet writer and add the necessary filters
            VariantParquetConverter parquetConverter = new VariantParquetConverter(
                    CompressionCodecName.fromConf(compressionCodecName), rowGroupSize, pageSize);
            parquetConverter.setDatasetName(variantCommandOptions.convertVariantCommandOptions.dataset);

            // valid id filter
            if (variantCommandOptions.convertVariantCommandOptions.validId) {
                parquetConverter.addValidIdFilter();
            }

//        // set minimum quality filter
//        if (variantCommandOptions.convertVariantCommandOptions.minQuality > 0) {
//            parquetConverter.addMinQualityFilter(variantCommandOptions.convertVariantCommandOptions.minQuality);
//        }

            // region filter management,
            // we use the same region list to store all regions from both parameter --regions and --region-file
            List<Region> regions = CliUtils.getRegionList(variantCommandOptions.convertVariantCommandOptions.regions,
                    variantCommandOptions.convertVariantCommandOptions.regionFilename);
            if (regions != null && regions.size() > 0) {
                parquetConverter.addRegionFilter(regions, false);
            }

            InputStream inputStream = new FileInputStream(inputPath.toString());
            if (fromAvro) {
                // convert to AVRO -> PARQUET
                System.out.println("\n\nStarting AVRO->PARQUET conversion...\n");
                startTime = System.currentTimeMillis();

                if (variantCommandOptions.convertVariantCommandOptions.numThreads > 1) {
                    parquetConverter.toParquetFromAvro(inputStream, output,
                            variantCommandOptions.convertVariantCommandOptions.numThreads);
                } else {
                    parquetConverter.toParquetFromAvro(inputStream, output);
                }

                elapsedTime = System.currentTimeMillis() - startTime;
                System.out.println("\n\nFinish AVRO->PARQUET conversion in " + (elapsedTime / 1000F) + " sec\n");
            } else {
                // convert to VCF -> PARQUET
                System.out.println("\n\nStarting VCF->PARQUET conversion...\n");
                startTime = System.currentTimeMillis();
                parquetConverter.toParquetFromVcf(inputPath.toString(), output, annotate);
                elapsedTime = System.currentTimeMillis() - startTime;
                System.out.println("\n\nFinish VCF->PARQUET conversion in " + (elapsedTime / 1000F) + " sec\n");
            }
        } else {
            // convert to VCF -> AVRO

            // create the Avro writer and add the necessary filters
            VariantAvroSerializer avroSerializer = new VariantAvroSerializer(
                    variantCommandOptions.convertVariantCommandOptions.species,
                    variantCommandOptions.convertVariantCommandOptions.assembly,
                    variantCommandOptions.convertVariantCommandOptions.dataset,
                    compressionCodecName);

            // valid id filter
            if (variantCommandOptions.convertVariantCommandOptions.validId) {
                avroSerializer.addValidIdFilter();
            }

//        // set minimum quality filter
//        if (variantCommandOptions.convertVariantCommandOptions.minQuality > 0) {
//            avroSerializer.addMinQualityFilter(variantCommandOptions.convertVariantCommandOptions.minQuality);
//        }

            // region filter management,
            // we use the same region list to store all regions from both parameter --regions and --region-file
            List<Region> regions = CliUtils.getRegionList(variantCommandOptions.convertVariantCommandOptions.regions,
                    variantCommandOptions.convertVariantCommandOptions.regionFilename);
            if (regions != null && regions.size() > 0) {
                avroSerializer.addRegionFilter(regions, false);
            }

            System.out.println("\n\nStarting VCF->AVRO conversion...\n");
            startTime = System.currentTimeMillis();
            avroSerializer.toAvro(inputPath.toString(), output, annotate);
            elapsedTime = System.currentTimeMillis() - startTime;
            System.out.println("\n\nFinish VCF->AVRO conversion in " + (elapsedTime / 1000F) + " sec\n");
        }
    }

    public void query() throws Exception {
        // sanity check: input file
        //Path inputPath = Paths.get(variantCommandOptions.queryVariantCommandOptions.input);
        //FileUtils.checkFile(inputPath);

        SparkConf sparkConf = SparkConfCreator.getConf("variant query", "local", 1, true);
        logger.debug("sparkConf = {}", sparkConf.toDebugString());
        SparkSession sparkSession = new SparkSession(new SparkContext(sparkConf));

        VariantDataset vd = new VariantDataset(sparkSession);
        vd.load(variantCommandOptions.queryVariantCommandOptions.input);
        vd.createOrReplaceTempView("vcf");

        // add filters
        CliUtils.addVariantFilters(variantCommandOptions, vd);

        // apply previous filters
        if (StringUtils.isNotEmpty(variantCommandOptions.queryVariantCommandOptions.groupBy)) {
            vd.countBy(variantCommandOptions.queryVariantCommandOptions.groupBy);
        } else {
            vd.update();
        }

        // save the dataset
        String output = variantCommandOptions.queryVariantCommandOptions.output;
        if (output != null) {
            if (output.endsWith(".json")) {
                CliUtils.saveDatasetAsOneFile(vd, "json", output, logger);
            } else if (output.endsWith(".parquet")) {
                CliUtils.saveDatasetAsOneFile(vd, "parquet", output, logger);
            } else {
                CliUtils.saveDatasetAsOneFile(vd, "avro", output, logger);
            }
        }

        // show output records
        if (variantCommandOptions.queryVariantCommandOptions.limit > 0) {
            vd.show(variantCommandOptions.queryVariantCommandOptions.limit);
        }

        // count output records
        if (variantCommandOptions.queryVariantCommandOptions.count) {
            long count = vd.count();
            System.out.println("----------------------------------------------");
            System.out.println("Number of output records: " + count);
            System.out.println("----------------------------------------------");
        }
    }

    public void annotate() throws IOException {
        VariantAvroAnnotator variantAvroAnnotator = new VariantAvroAnnotator();

        Path input = get(variantCommandOptions.annotateVariantCommandOptions.input);
        Path output = get(variantCommandOptions.annotateVariantCommandOptions.ouput);
        variantAvroAnnotator.annotate(input, output);
    }

    public void view() throws Exception {
        Path input = get(variantCommandOptions.viewVariantCommandOptions.input);
        int head = variantCommandOptions.viewVariantCommandOptions.head;

        // open
        InputStream is = new FileInputStream(input.toFile());
        DataFileStream<VariantAvro> reader = new DataFileStream<>(is,
                new SpecificDatumReader<>(VariantAvro.class));

        long counter = 0;
        ObjectMapper mapper = new ObjectMapper();
        if (variantCommandOptions.viewVariantCommandOptions.vcf) {
            // vcf format
            // first, header

            // and then, variant
            System.err.println("Warning: VCF output format is not implemented yet!");
        } else if (variantCommandOptions.viewVariantCommandOptions.schema) {
            // schema
            System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(
                    mapper.readValue(reader.getSchema().toString(), Object.class)));
        } else {
            // main
            System.out.println("[");
            for (VariantAvro variant : reader) {
                // remove annotations ?
                if (variantCommandOptions.viewVariantCommandOptions.excludeAnnotations) {
                    variant.setAnnotation(null);
                }

                // remove samples ?
                if (variantCommandOptions.viewVariantCommandOptions.excludeSamples) {
                    for (StudyEntry study: variant.getStudies()) {
                        study.setSamplesData(null);
                    }
                }
                System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(
                        mapper.readValue(variant.toString(), Object.class)));
                counter++;
                if (head > 0 && counter == head) {
                    break;
                }
                System.out.println(",");
            }
            System.out.println("]");
        }

        // close
        reader.close();
    }

    public void metadata() throws Exception {
        // sanity check
        Path input = get(variantCommandOptions.metadataVariantCommandOptions.input);
        String datasetId = variantCommandOptions.metadataVariantCommandOptions.datasetId;

        boolean updated = false;

        // metadata file management
        File metaFile = new File(input.toString() + ".meta.json");
        if (metaFile.exists()) {

            List<Pedigree> pedigrees = null;
            VariantMetadataManager metadataManager = new VariantMetadataManager();
            metadataManager.load(metaFile.toPath());

            // load pedigree ?
            if (variantCommandOptions.metadataVariantCommandOptions.loadPedFilename != null) {
                pedigrees = new PedigreeManager().parse(
                        get(variantCommandOptions.metadataVariantCommandOptions.loadPedFilename));

                //System.out.println(pedigree.toString());

                metadataManager.loadPedigree(pedigrees, datasetId);
                updated = true;
            }

            // save pedigree ?
            if (variantCommandOptions.metadataVariantCommandOptions.savePedFilename != null) {
                pedigrees = metadataManager.getPedigree(datasetId);
                new PedigreeManager().save(pedigrees,
                        get(variantCommandOptions.metadataVariantCommandOptions.savePedFilename));

            }

            // create cohort ?
            if (variantCommandOptions.metadataVariantCommandOptions.createCohort != null) {
                String[] names = variantCommandOptions.metadataVariantCommandOptions.createCohort.split("::");

                List<String> sampleIds;
                if (new File(names[1]).exists()) {
                    sampleIds = Files.readAllLines(Paths.get(names[1]));
                } else {
                    sampleIds = Arrays.asList(StringUtils.split(names[1], ","));
                }

                Cohort cohort = new Cohort(names[0], sampleIds, SampleSetType.MISCELLANEOUS);
                metadataManager.addCohort(cohort, datasetId);
                updated = true;
            }

            // summary ?
            if (variantCommandOptions.metadataVariantCommandOptions.summary) {
                metadataManager.printSummary();
            }

            if (updated) {
                // overwrite the metadata
                if (metaFile.exists()) {
                    metaFile.delete();
                }
                metadataManager.save(metaFile.toPath(), true);
            }
        } else {
            System.out.println("Error: metafile does not exist, " + metaFile.getAbsolutePath());
        }
    }

    public void rvtests() throws Exception {
        Query query = null;
        if (variantCommandOptions.rvtestsVariantCommandOptions.variantFilterOptions != null) {
            query = LocalCliOptionsParser.variantFilterOptionsParser(
                    variantCommandOptions.rvtestsVariantCommandOptions.variantFilterOptions);
        }

        RvTestsWrapper rvtests = new RvTestsWrapper(variantCommandOptions.rvtestsVariantCommandOptions.datasetId,
                variantCommandOptions.rvtestsVariantCommandOptions.inFilename,
                variantCommandOptions.rvtestsVariantCommandOptions.inFilename + ".meta.json",
                query, variantCommandOptions.rvtestsVariantCommandOptions.rvtestsParams);

        // Get the binary path from input parameter
        String binPath = variantCommandOptions.rvtestsVariantCommandOptions.binPath;
        if (!StringUtils.isEmpty(binPath)) {
            rvtests.setBinPath(Paths.get(binPath));
        }
        rvtests.execute();
    }

    public void plink() throws Exception {
        Query query = null;
        if (variantCommandOptions.rvtestsVariantCommandOptions.variantFilterOptions != null) {
            query = LocalCliOptionsParser.variantFilterOptionsParser(
                    variantCommandOptions.rvtestsVariantCommandOptions.variantFilterOptions);
        }

        PlinkWrapper plink = new PlinkWrapper(variantCommandOptions.plinkVariantCommandOptions.datasetId,
                variantCommandOptions.plinkVariantCommandOptions.inFilename,
                variantCommandOptions.plinkVariantCommandOptions.inFilename + ".meta.json",
                query, variantCommandOptions.plinkVariantCommandOptions.plinkParams);

        // Get the binary path from input parameter
        String binPath = variantCommandOptions.plinkVariantCommandOptions.binPath;
        if (!StringUtils.isEmpty(binPath)) {
            plink.setBinPath(Paths.get(binPath));
        }
        plink.execute();
    }
}
