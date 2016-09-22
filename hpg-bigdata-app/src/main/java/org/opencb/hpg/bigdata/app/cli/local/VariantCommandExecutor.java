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

import com.fasterxml.jackson.databind.ObjectMapper;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.StudyEntry;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.biodata.models.variant.avro.VariantFileMetadata;
import org.opencb.biodata.tools.variant.converter.VariantContextToVariantConverter;
import org.opencb.commons.utils.FileUtils;
import org.opencb.hpg.bigdata.app.cli.CommandExecutor;
import org.opencb.hpg.bigdata.core.avro.VariantAvroAnnotator;
import org.opencb.hpg.bigdata.core.avro.VariantAvroSerializer;
import org.opencb.hpg.bigdata.core.converters.variation.VariantContext2VariantConverter;
import org.opencb.hpg.bigdata.core.lib.SparkConfCreator;
import org.opencb.hpg.bigdata.core.lib.VariantDataset;
import org.opencb.hpg.bigdata.core.parquet.VariantParquetConverter;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.nio.file.Paths.get;

/**
 * Created by imedina on 25/06/15.
 */
public class VariantCommandExecutor extends CommandExecutor {

    private LocalCliOptionsParser.VariantCommandOptions variantCommandOptions;

    public VariantCommandExecutor(LocalCliOptionsParser.VariantCommandOptions variantCommandOptions) {
//      super(variantCommandOptions.c, fastqCommandOptions.verbose, fastqCommandOptions.conf);
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
            case "annotate":
                init(variantCommandOptions.convertVariantCommandOptions.commonOptions.logLevel,
                        variantCommandOptions.convertVariantCommandOptions.commonOptions.verbose,
                        variantCommandOptions.convertVariantCommandOptions.commonOptions.conf);
                annotate();
                break;
            case "view":
                init(variantCommandOptions.viewVariantCommandOptions.commonOptions.logLevel,
                        variantCommandOptions.viewVariantCommandOptions.commonOptions.verbose,
                        variantCommandOptions.viewVariantCommandOptions.commonOptions.conf);
                view();
                break;
            case "query":
                init(variantCommandOptions.queryVariantCommandOptions.commonOptions.logLevel,
                        variantCommandOptions.queryVariantCommandOptions.commonOptions.verbose,
                        variantCommandOptions.queryVariantCommandOptions.commonOptions.conf);
                query();
                break;
            case "metadata":
                init(variantCommandOptions.queryVariantCommandOptions.commonOptions.logLevel,
                        variantCommandOptions.queryVariantCommandOptions.commonOptions.verbose,
                        variantCommandOptions.queryVariantCommandOptions.commonOptions.conf);
                metadata();
                break;
            default:
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
        String output = Utils.getOutputFilename(variantCommandOptions.convertVariantCommandOptions.input,
                variantCommandOptions.convertVariantCommandOptions.output, to);

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
            List<Region> regions = Utils.getRegionList(variantCommandOptions.convertVariantCommandOptions.regions,
                    variantCommandOptions.convertVariantCommandOptions.regionFilename);
            if (regions != null && regions.size() > 0) {
                parquetConverter.addRegionFilter(regions, false);
            }

            InputStream inputStream = new FileInputStream(inputPath.toString());
            if (fromAvro) {
                // convert to AVRO -> PARQUET
                System.out.println("\n\nStarting AVRO->PARQUET conversion...\n");
                startTime = System.currentTimeMillis();
                parquetConverter.toParquetFromAvro(inputStream, output);
                elapsedTime = System.currentTimeMillis() - startTime;
                System.out.println("\n\nFinish AVRO->PARQUET conversion in " + (elapsedTime / 1000F) + " sec\n");

                // metadata file management
                File metaFile = new File(inputPath.toString() + ".meta.json");
                if (metaFile.exists()) {
                    File outMetaFile = new File(output + ".meta.json");

                    // read metadata JSON to update filename
                    ObjectMapper mapper = new ObjectMapper();
                    //mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
                    VariantFileMetadata data = mapper.readValue(metaFile, VariantFileMetadata.class);
                    data.setFileName(outMetaFile.toString());

                    // write the metadata
                    PrintWriter writer = new PrintWriter(new FileOutputStream(outMetaFile));
                    writer.write(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(
                            mapper.readValue(data.toString(), Object.class)));
                    writer.close();
                }
            } else {
                // convert to VCF -> PARQUET
                System.out.println("\n\nStarting VCF->PARQUET conversion...\n");
                startTime = System.currentTimeMillis();
                parquetConverter.toParquetFromVcf(inputStream, output);
                elapsedTime = System.currentTimeMillis() - startTime;
                System.out.println("\n\nFinish VCF->PARQUET conversion in " + (elapsedTime / 1000F) + " sec\n");
            }
        } else {
            // convert to VCF -> AVRO

            // create the Avro writer and add the necessary filters
            VariantAvroSerializer avroSerializer = new VariantAvroSerializer(compressionCodecName);

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
            List<Region> regions = Utils.getRegionList(variantCommandOptions.convertVariantCommandOptions.regions,
                    variantCommandOptions.convertVariantCommandOptions.regionFilename);
            if (regions != null && regions.size() > 0) {
                avroSerializer.addRegionFilter(regions, false);
            }

            System.out.println("\n\nStarting VCF->AVRO conversion...\n");
            startTime = System.currentTimeMillis();
            avroSerializer.toAvro(inputPath.toString(), output);
            elapsedTime = System.currentTimeMillis() - startTime;
            System.out.println("\n\nFinish VCF->AVRO conversion in " + (elapsedTime / 1000F) + " sec\n");
        }
    }

//    private void convert3() throws Exception {
//        // check mandatory parameter 'input file'
//        Path inputPath = Paths.get(variantCommandOptions.convertVariantCommandOptions.input);
//        FileUtils.checkFile(inputPath);
//
//        // check mandatory parameter 'to'
//        String to = variantCommandOptions.convertVariantCommandOptions.to;
//        if (!to.equals("avro") && !to.equals("parquet") && !to.equals("json")) {
//            throw new IllegalArgumentException("Unknown serialization format: " + to + ". Valid values: avro, parquet and json");
//        }
//
//        // check output
//        String output = variantCommandOptions.convertVariantCommandOptions.output;
//        boolean stdOutput = variantCommandOptions.convertVariantCommandOptions.stdOutput;
//        OutputStream outputStream;
//        if (stdOutput) {
//            output = "STDOUT";
//        } else {
//            if (output != null && !output.isEmpty()) {
//                Path parent = Paths.get(output).toAbsolutePath().getParent();
//                if (parent != null) { // null if output is a file in the current directory
//                    FileUtils.checkDirectory(parent, true); // Throws exception, if does not exist
//                }
//            } else {
//                output = inputPath.toString() + "." + to;
//            }
//            outputStream = new FileOutputStream(output);
//        }
//
//        // compression
//        String compression = variantCommandOptions.convertVariantCommandOptions.compression;
//
//        // region filter
//        List<Region> regions = null;
//        if (StringUtils.isNotEmpty(variantCommandOptions.convertVariantCommandOptions.regions)) {
//            regions = Region.parseRegions(variantCommandOptions.convertVariantCommandOptions.regions);
//        }
//
//        switch (variantCommandOptions.convertVariantCommandOptions.to) {
//            case "avro":
//                VariantParquetSerializer avroSerializer = new VariantParquetSerializer(compression);
//                if (regions != null) {
//                    regions.forEach(avroSerializer::addRegionFilter);
//                }
//                avroSerializer.toAvro(inputPath.toString(), output);
//                break;
//            case "parquet":
//                InputStream is = new FileInputStream(variantCommandOptions.convertVariantCommandOptions.input);
//                VariantParquetConverter parquetConverter = new VariantParquetConverter();
//                parquetConverter.toParquet(is, variantCommandOptions.convertVariantCommandOptions.output + "2");
//                break;
//            default:
//                System.out.println("No valid format: " + variantCommandOptions.convertVariantCommandOptions.to);
//                break;
//        }
//
//    }

//    private void convert2() throws Exception {
//        Path inputPath = Paths.get(variantCommandOptions.convertVariantCommandOptions.input);
//        FileUtils.checkFile(inputPath);
//
//        // Creating file writer. If 'output' parameter is passed and it is different from
//        // STDOUT then a file is created if parent folder exist, otherwise STDOUT is used.
//        String output = variantCommandOptions.convertVariantCommandOptions.output;
//        boolean isFile = false;
//        OutputStream outputStream;
//        if (output != null && !output.isEmpty() && !output.equalsIgnoreCase("STDOUT")) {
//            Path parent = Paths.get(output).toAbsolutePath().getParent();
//            if (parent != null) { // null if output is a file in the current directory
//                FileUtils.checkDirectory(parent, true); // Throws exception, if does not exist
//            }
//            outputStream = new FileOutputStream(output);
//            isFile = true;
//        } else {
//            outputStream = System.out;
//            output = "STDOUT";
//        }
//
//        String dataModel = variantCommandOptions.convertVariantCommandOptions.dataModel;
//        dataModel = (dataModel != null && !dataModel.isEmpty()) ? dataModel : "opencb";
//
//        String compression = variantCommandOptions.convertVariantCommandOptions.compression;
//        compression = (compression == null || compression.isEmpty()) ? "auto" :  compression.toLowerCase();
//
//        if (!variantCommandOptions.convertVariantCommandOptions.toJson
//                && !variantCommandOptions.convertVariantCommandOptions.toAvro
//                && !variantCommandOptions.convertVariantCommandOptions.toProtoBuf
//                && !variantCommandOptions.convertVariantCommandOptions.fromAvro) {
////            variantCommandOptions.convertVariantCommandOptions.toAvro = true;
//            variantCommandOptions.convertVariantCommandOptions.toParquet = true;
//        }
//
//        /*
//         * JSON converter. Mode 'auto' set to gzip is file name ends with .gz
//         */
//        if (variantCommandOptions.convertVariantCommandOptions.toJson) {
//            if (compression.equals("auto")) {
//                if (output.endsWith(".gz")) {
//                    compression = "gzip";
//                } else if (output.equalsIgnoreCase("STDOUT") || output.endsWith("json")) {
//                    compression = "";
//                } else {
//                    throw new IllegalArgumentException("Unknown compression extension for " + output);
//                }
//            }
//
//            if (compression.equals("gzip")) {
//                outputStream = new GZIPOutputStream(outputStream);
//            }
//            convertToJson(inputPath, dataModel, outputStream);
//        }
//
//        /*
//         * Protocol Buffer 3 converter. Mode 'auto' set to gzip is file name ends with .gz
//         */
//        if (variantCommandOptions.convertVariantCommandOptions.toProtoBuf) {
//            if (compression.equals("auto")) {
//                if (output.endsWith(".gz")) {
//                    compression = "gzip";
//                } else if (output.equalsIgnoreCase("STDOUT")
//                        || output.endsWith("pb")
//                        || output.endsWith("pb3")
//                        || output.endsWith("proto")) {
//                    compression = "";
//                } else {
//                    throw new IllegalArgumentException("Unknown compression extension for " + output);
//                }
//            }
//
//            if (compression.equals("gzip")) {
//                outputStream = new GZIPOutputStream(outputStream);
//            }
//            convertToProtoBuf(inputPath, outputStream);
//        }
//
//        /*
//         * Avro converter. Mode 'auto' set to gzip is file name ends with .gz
//         */
//        if (variantCommandOptions.convertVariantCommandOptions.toAvro) {
//            // if compression mode is set to 'auto' it is inferred from files extension
//            if (compression.equals("auto")) {
//                // if output is a defined file and contains an extension
//                if (output.contains(".")) {
//                    String[] split = output.split("\\.");
//                    switch (split[split.length - 1]) {
//                        case "gz":
//                        case "deflate":
//                            compression = "deflate";
//                            break;
//                        case "sz":
//                        case "snz":
//                            compression = "snappy";
//                            break;
//                        case "bzip2":
//                            compression = "bzip2";
//                            break;
//                        case "xz":
//                            compression = "xz";
//                            break;
//                        default:
//                            compression = "deflate";
//                            break;
//                    }
//                } else {    // if we reach this point is very likely output is set to STDOUT
//                    compression = "deflate";
//                }
//            }
//
//            System.out.println("compression = " + compression);
//            VariantParquetSerializer avroSerializer = new VariantParquetSerializer(compression);
//            avroSerializer.toAvro(inputPath.toString(), output);
//
//            /*
//            convertToAvro(inputPath, compression, dataModel, outputStream);
//
//            if (isFile) {
//                String metaFile = output + ".meta";
//                logger.info("Write metadata into " + metaFile);
//                try (FileOutputStream out = new FileOutputStream(metaFile)) {
//                    writeAvroStats(new AvroFileWriter<>(VariantFileMetadata.getClassSchema(), compression, out), output);
//                }
//            }
//            */
//        }
//
//        if (variantCommandOptions.convertVariantCommandOptions.toParquet) {
//            InputStream is = new FileInputStream(variantCommandOptions.convertVariantCommandOptions.input);
//            VariantParquetConverter parquetConverter = new VariantParquetConverter();
////            parquetConverter.addRegionFilter(new Region("1", 1, 800000))
////                    .addRegionFilter(new Region("1", 798801, 222800000))
////                    .addFilter(v -> v.getStudies().get(0).getFiles().get(0).getAttributes().get("NS").equals("60"));
//            parquetConverter.toParquet(is, variantCommandOptions.convertVariantCommandOptions.output + "2");
//
//            is.close();
//        }
//
//        if (outputStream != null) {
//            outputStream.flush();
//            outputStream.close();
//        }
//    }

    private void convertToJson(Path inputPath, String dataModel, OutputStream outputStream) throws IOException {
        VCFFileReader reader = new VCFFileReader(inputPath.toFile(), false);
        switch (dataModel.toLowerCase()) {
            case "opencb": {
                VariantContextToVariantConverter variantContextToVariantConverter =
                        new VariantContextToVariantConverter("", "", Collections.emptyList());
                Variant variant;
                for (VariantContext variantContext : reader) {
                    variant = variantContextToVariantConverter.convert(variantContext);
                    outputStream.write(variant.toJson().getBytes());
                    outputStream.write('\n');
                }
                break;
            }
            case "ga4gh": {
                // GA4GH Avro data models used
                VariantContext2VariantConverter variantContext2VariantConverter = new VariantContext2VariantConverter();
                org.ga4gh.models.Variant variant;
                for (VariantContext variantContext : reader) {
                    variant = variantContext2VariantConverter.forward(variantContext);
                    outputStream.write(variant.toString().getBytes());
                    outputStream.write('\n');
                }
                break;
            }
            default:
                throw new IllegalArgumentException("Unknown dataModel \"" + dataModel + "\"");
        }
        reader.close();
    }

    private void convertToProtoBuf(Path inputPath, OutputStream outputStream) throws Exception {
//        // Creating reader
//        VcfBlockIterator iterator = (StringUtils.equals("-", inputPath.toAbsolutePath().toString()))
//                ? new VcfBlockIterator(new BufferedInputStream(System.in), new FullVcfCodec())
//                : new VcfBlockIterator(inputPath.toFile(), new FullVcfCodec());
//
//
//        LocalCliOptionsParser.ConvertVariantCommandOptions cliOptions = variantCommandOptions.convertVariantCommandOptions;
//        int numTasks = Math.max(cliOptions.numThreads, 1);
//        int batchSize = Integer.parseInt(cliOptions.options.getOrDefault("batch.size", "50"));
//        int bufferSize = Integer.parseInt(cliOptions.options.getOrDefault("buffer.size", "100000"));
//        int capacity = numTasks + 1;
//        ParallelTaskRunner.Config config = new ParallelTaskRunner.Config(numTasks, batchSize, capacity, true, false);
//
//        ParallelTaskRunner<CharSequence, ByteBuffer> runner = new ParallelTaskRunner<>(
//                iterator.toLineDataReader(),
//                () -> { //Task supplier. Will supply a task instance for each thread.
//
//                    //VCFCodec is not thread safe. MUST exist one instance per thread
//                    VCFCodec codec = new FullVcfCodec(iterator.getHeader(), iterator.getVersion());
//                    VariantContextBlockIterator blockIterator = new VariantContextBlockIterator(codec);
//                    Converter<VariantContext, VariantProto.Variant> converter = new VariantContextToVariantProtoConverter();
//                    return new ProtoEncoderTask<>(charBuffer -> converter.convert(blockIterator.convert(charBuffer)), bufferSize);
//                },
//                batch -> {
//                    batch.forEach(byteBuffer -> {
//                        try {
//                            outputStream.write(byteBuffer.array(), byteBuffer.arrayOffset(), byteBuffer.limit());
//                        } catch (IOException e) {
//                            throw new RuntimeException(e);
//                        }
//                    });
//                    return true;
//                }, config
//        );
//        runner.run();
//        outputStream.close();

//        InputStream inputStream = new FileInputStream(variantCommandOptions.convertVariantCommandOptions.output);
//        if (outputStream instanceof GZIPOutputStream) {
//            inputStream = new GZIPInputStream(inputStream);
//        }
//        VariantProto.Variant variant;
//        int i = 0;
//        try {
//            while ((variant = VariantProto.Variant.parseDelimitedFrom(inputStream)) != null) {
//                i++;
//            System.out.println(variant.getChromosome() + ":" + variant.getStart()
//                    + ":" + variant.getReference() + ":" + variant.getAlternate());
////            System.out.println("variant = " + variant.toString());
//            }
//        } finally {
//            System.out.println("Num variants = " + i);
//            inputStream.close();
//        }
    }

    /*

    private void convertToAvro2(Path inputPath, String compression, String dataModel, OutputStream outputStream) throws Exception {

        VariantContextToVariantConverter converter = new VariantContextToVariantConverter("", "");
        VCFFileReader vcfFileReader = new VCFFileReader(inputPath.toFile(), false);
        VCFHeader fileHeader = vcfFileReader.getFileHeader();
        CloseableIterator<VariantContext> iterator = vcfFileReader.iterator();
        while (iterator.hasNext()) {
            VariantContext variantContext = iterator.next();
            System.out.println("======================================");
            System.out.println("variantContext = " + variantContext);
            System.out.println("variantContext.getCommonInfo().getAttributes() = " + variantContext.getCommonInfo().getAttributes());
            System.out.println("variantContext.getGenotypes().isLazyWithData() = " + variantContext.getGenotypes().isLazyWithData());
            ((LazyGenotypesContext)variantContext.getGenotypes()).decode();
            System.out.println("variantContext.getGenotypes().getUnparsedGenotypeData() = "
                    + ((LazyGenotypesContext)variantContext.getGenotypes()).getUnparsedGenotypeData());
//            System.out.println("variantContext.toStringDecodeGenotypes() = " + variantContext.toStringDecodeGenotypes());
            System.out.println("variantContext.getGenotypes().get(0) = " + variantContext.getGenotypes().get(0).hasAnyAttribute("GC"));
            System.out.println("variantContext.getGenotypes().get(0).getExtendedAttributes() = " + variantContext.getGenotypes().get(0)
                    .getExtendedAttributes());
            Variant variant = converter.convert(variantContext);
            System.out.println("variant = " + variant);
            System.out.println("======================================");
        }
    }
     */

    private void convertToAvro(Path inputPath, String compression, String dataModel, OutputStream outputStream) throws Exception {
//        // Creating reader
//        VcfBlockIterator iterator = (StringUtils.equals("-", inputPath.toAbsolutePath().toString()))
//                ? new VcfBlockIterator(new BufferedInputStream(System.in), new FullVcfCodec())
//                : new VcfBlockIterator(inputPath.toFile(), new FullVcfCodec());
//        DataReader<CharBuffer> vcfDataReader = iterator.toCharBufferDataReader();
//
//
//        ArrayList<String> sampleNamesInOrder = iterator.getHeader().getSampleNamesInOrder();
////        System.out.println("sampleNamesInOrder = " + sampleNamesInOrder);
//
//        // main loop
//        int numTasks = Math.max(variantCommandOptions.convertVariantCommandOptions.numThreads, 1);
//        int batchSize = 1024 * 1024;  //Batch size in bytes
//        int capacity = numTasks + 1;
////            VariantConverterContext variantConverterContext = new VariantConverterContext();
//
////        long start = System.currentTimeMillis();
//
////        final VariantContextToVariantConverter converter = new VariantContextToVariantConverter("", "", sampleNamesInOrder);
////        List<CharBuffer> read;
////        while ((read = vcfDataReader.read()) != null {
////            converter.convert(read.)
////        }
//
////        Old implementation:
//
//        ParallelTaskRunner.Config config = new ParallelTaskRunner.Config(numTasks, batchSize, capacity, false);
//        ParallelTaskRunner<CharBuffer, ByteBuffer> runner;
//        switch (dataModel.toLowerCase()) {
//            case "opencb": {
//                Schema classSchema = VariantAvro.getClassSchema();
//                // Converter
//                final VariantContextToVariantConverter converter = new VariantContextToVariantConverter("", "", sampleNamesInOrder);
//                // Writer
//                AvroFileWriter<VariantAvro> avroFileWriter = new AvroFileWriter<>(classSchema, compression, outputStream);
//
//                runner = new ParallelTaskRunner<>(
//                        vcfDataReader,
//                        () -> new VariantAvroEncoderTask<>(iterator.getHeader(), iterator.getVersion(),
//                                variantContext -> converter.convert(variantContext).getImpl(), classSchema),
//                        avroFileWriter, config);
//                break;
//            }
//            case "ga4gh": {
//                Schema classSchema = org.ga4gh.models.Variant.getClassSchema();
//                // Converter
//                final VariantContext2VariantConverter converter = new VariantContext2VariantConverter();
//                converter.setVariantSetId("");  //TODO: Set VariantSetId
//                // Writer
//                AvroFileWriter<org.ga4gh.models.Variant> avroFileWriter = new AvroFileWriter<>(classSchema, compression, outputStream);
//
//                runner = new ParallelTaskRunner<>(
//                        vcfDataReader,
//                        () -> new VariantAvroEncoderTask<>(iterator.getHeader(), iterator.getVersion(), converter, classSchema),
//                        avroFileWriter, config);
//                break;
//            }
//            default:
//                throw new IllegalArgumentException("Unknown dataModel \"" + dataModel + "\"");
//        }
//        long start = System.currentTimeMillis();
//        runner.run();
//
//        logger.debug("Time " + (System.currentTimeMillis() - start) / 1000.0 + "s");
    }
/*
    private void writeAvroStats(AvroFileWriter<VariantFileMetadata> aw, String file) throws IOException {
        try {
            aw.open();
            Builder builder = VariantFileMetadata.newBuilder();
            builder.setStudyId(file).setFileId(file);
            Map<String, Object> meta = new HashMap<>();
            meta.put("FILTER_DEFAULT", "PASS");
            meta.put("QUAL_DEFAULT", StringUtils.EMPTY);
            meta.put("INFO_DEFAULT", "END,BLOCKAVG_min30p3a");
            meta.put("FORMAT_DEFAULT", "GT:GQX:DP:DPF");
            builder.setMetadata(meta);
            builder.setAggregation(Aggregation.NONE);
            builder.setStats(null);
            builder.setHeader(null);
            aw.writeDatum(builder.build());
        } finally {
            try {
                aw.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
*/

    public void query() throws Exception {
        // sanity check: input file
        Path inputPath = Paths.get(variantCommandOptions.queryVariantCommandOptions.input);
        FileUtils.checkFile(inputPath);

        SparkConf sparkConf = SparkConfCreator.getConf("variant query", "local", 1, true);
        logger.debug("sparkConf = {}", sparkConf.toDebugString());
        SparkSession sparkSession = new SparkSession(new SparkContext(sparkConf));

        VariantDataset vd = new VariantDataset(sparkSession);
        vd.load(variantCommandOptions.queryVariantCommandOptions.input);
        vd.createOrReplaceTempView("vcf");

        // query for ID (list and file)
        List<String> list = null;
        if (StringUtils.isNotEmpty(variantCommandOptions.queryVariantCommandOptions.ids)) {
            list = new ArrayList<>(Arrays.asList(
                    StringUtils.split(variantCommandOptions.queryVariantCommandOptions.ids, ",")));
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
            vd.typeFilter(new ArrayList<>(Arrays.asList(
                    StringUtils.split(variantCommandOptions.queryVariantCommandOptions.types, ","))));
        }

        // query for biotype
        if (StringUtils.isNotEmpty(variantCommandOptions.queryVariantCommandOptions.biotypes)) {
            vd.annotationFilter("biotype", new ArrayList<>(Arrays.asList(
                    StringUtils.split(variantCommandOptions.queryVariantCommandOptions.biotypes, ","))));
        }

        // query for study
        if (StringUtils.isNotEmpty(variantCommandOptions.queryVariantCommandOptions.studies)) {
            vd.studyFilter("studyId", new ArrayList<>(Arrays.asList(
                    StringUtils.split(variantCommandOptions.queryVariantCommandOptions.studies, ","))));
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
        List<Region> regions = Utils.getRegionList(variantCommandOptions.queryVariantCommandOptions.regions,
                variantCommandOptions.queryVariantCommandOptions.regionFilename);
        if (regions != null && regions.size() > 0) {
            vd.regionFilter(regions);
        }

        // query for consequence type (Sequence Ontology term names and accession codes)
//        if (StringUtils.isNotEmpty(variantCommandOptions.queryVariantCommandOptions.consequenceTypes)) {
////            vd.annotationFilter("consequenceTypes.sequenceOntologyTerms", new ArrayList<>(Arrays.asList(
////                    StringUtils.split(variantCommandOptions.queryVariantCommandOptions.consequenceTypes, ","))));
//            vd.annotationFilter("consequenceTypes.sequenceOntologyTerms",
// variantCommandOptions.queryVariantCommandOptions.consequenceTypes);
//        }
        annotationFilterNotEmpty("consequenceTypes.sequenceOntologyTerms",
                variantCommandOptions.queryVariantCommandOptions.consequenceTypes, vd);

        // query for clinvar (accession)
        if (StringUtils.isNotEmpty(variantCommandOptions.queryVariantCommandOptions.clinvar)) {
            vd.annotationFilter("variantTraitAssociation.clinvar.accession", new ArrayList<>(Arrays.asList(
                    StringUtils.split(variantCommandOptions.queryVariantCommandOptions.clinvar, ","))));
        }

        // query for cosmic (mutation ID)
        if (StringUtils.isNotEmpty(variantCommandOptions.queryVariantCommandOptions.cosmic)) {
            vd.annotationFilter("variantTraitAssociation.cosmic.mutationId", new ArrayList<>(Arrays.asList(
                    StringUtils.split(variantCommandOptions.queryVariantCommandOptions.cosmic, ","))));
        }

        // query for conservation (phastCons, phylop, gerp)
        if (StringUtils.isNotEmpty(variantCommandOptions.queryVariantCommandOptions.conservScores)) {
            vd.annotationFilter("conservation", new ArrayList<>(Arrays.asList(
                    StringUtils.split(variantCommandOptions.queryVariantCommandOptions.conservScores, ","))));
        }

        // query for protein substitution scores (polyphen, sift)
        if (StringUtils.isNotEmpty(variantCommandOptions.queryVariantCommandOptions.substScores)) {
            vd.annotationFilter("proteinVariantAnnotation.substitutionScores", new ArrayList<>(Arrays.asList(
                    StringUtils.split(variantCommandOptions.queryVariantCommandOptions.substScores, ","))));
        }

        // query for alternate population frequency (study:population)
        if (StringUtils.isNotEmpty(variantCommandOptions.queryVariantCommandOptions.pf)) {
            vd.annotationFilter("populationFrequencies.altAlleleFreq", variantCommandOptions.queryVariantCommandOptions.pf);
        }

        // query for population minor allele frequency (study:population)
        if (StringUtils.isNotEmpty(variantCommandOptions.queryVariantCommandOptions.pmaf)) {
            vd.annotationFilter("populationFrequencies.refAlleleFreq", variantCommandOptions.queryVariantCommandOptions.pmaf);
        }

        // apply previous filters
        vd.update();

        // save the dataset
        String output = variantCommandOptions.queryVariantCommandOptions.output;
        if (output.endsWith(".json")) {
            Utils.saveDatasetAsOneFile(vd, "json", output, logger);
        } else if (output.endsWith(".parquet")) {
            Utils.saveDatasetAsOneFile(vd, "parquet", output, logger);
        } else {
            Utils.saveDatasetAsOneFile(vd, "avro", output, logger);
        }

        // show output records
        if (variantCommandOptions.queryVariantCommandOptions.show > 0) {
            vd.show(variantCommandOptions.queryVariantCommandOptions.show);
        }

        // count output records
        if (variantCommandOptions.queryVariantCommandOptions.count) {
            System.out.println("----------------------------------------------");
            System.out.println("Number of output records: " + vd.count());
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


        // metadata file management
        File metaFile = new File(input.toString() + ".meta.json");
        if (metaFile.exists()) {
            // read metadata JSON to update filename
            ObjectMapper mapper = new ObjectMapper();
            //mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
            VariantFileMetadata metadata = mapper.readValue(metaFile, VariantFileMetadata.class);

            if (variantCommandOptions.metadataVariantCommandOptions.pedigreeFilename != null) {
                System.out.println("Warning: load a pedigree file is not yet implemented !");
                System.exit(-1);
            }

            if (variantCommandOptions.metadataVariantCommandOptions.cohortName != null) {
                System.out.println("Warning: create cohort is not yet implemented !");
                System.exit(-1);
            }

            if (variantCommandOptions.metadataVariantCommandOptions.renameDataset != null) {
                System.out.println("Warning: rename dataset is not yet implemented !");
                System.exit(-1);
            }

            // write the metadata
            PrintWriter writer = new PrintWriter(new FileOutputStream(metaFile));
            writer.write(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(
                    mapper.readValue(metadata.toString(), Object.class)));
            writer.close();
        } else {
            System.out.println("Error: metafile does not exist, " + metaFile.getAbsolutePath());
        }
    }

    private void annotationFilterNotEmpty(String key, String value, VariantDataset vd) {
        if (StringUtils.isNotEmpty(value)) {
            vd.annotationFilter(key, value);
        }
    }
}
