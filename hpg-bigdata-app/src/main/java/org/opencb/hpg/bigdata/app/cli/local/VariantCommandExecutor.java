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

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFCodec;
import htsjdk.variant.vcf.VCFFileReader;
import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.biodata.models.variant.avro.VariantFileMetadata;
import org.opencb.biodata.models.variant.avro.VariantFileMetadata.Builder;
import org.opencb.biodata.models.variant.protobuf.VariantProto;
import org.opencb.biodata.tools.variant.converter.Converter;
import org.opencb.biodata.tools.variant.converter.VariantContextToVariantConverter;
import org.opencb.biodata.tools.variant.converter.VariantContextToVariantProtoConverter;
import org.opencb.commons.io.DataReader;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.commons.utils.FileUtils;
import org.opencb.hpg.bigdata.app.cli.CommandExecutor;
import org.opencb.hpg.bigdata.core.converters.FullVcfCodec;
import org.opencb.hpg.bigdata.core.converters.variation.ProtoEncoderTask;
import org.opencb.hpg.bigdata.core.converters.variation.VariantAvroEncoderTask;
import org.opencb.hpg.bigdata.core.converters.variation.VariantContext2VariantConverter;
import org.opencb.hpg.bigdata.core.io.VariantContextBlockIterator;
import org.opencb.hpg.bigdata.core.io.VcfBlockIterator;
import org.opencb.hpg.bigdata.core.io.avro.AvroFileWriter;

import java.io.BufferedInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

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
            default:
                break;
        }
    }

    private void convert() throws Exception {
        Path inputPath = Paths.get(variantCommandOptions.convertVariantCommandOptions.input);
        FileUtils.checkFile(inputPath);

        // Creating file writer. If 'output' parameter is passed and it is different from
        // STDOUT then a file is created if parent folder exist, otherwise STDOUT is used.
        String output = variantCommandOptions.convertVariantCommandOptions.output;
        boolean isFile = false;
        OutputStream outputStream;
        if (output != null && !output.isEmpty() && !output.equalsIgnoreCase("STDOUT")) {
            Path parent = Paths.get(output).toAbsolutePath().getParent();
            if (parent != null) { // null if output is a file in the current directory
                FileUtils.checkDirectory(parent, true); // Throws exception, if does not exist
            }
            outputStream = new FileOutputStream(output);
            isFile = true;
        } else {
            outputStream = System.out;
            output = "STDOUT";
        }

        String dataModel = variantCommandOptions.convertVariantCommandOptions.dataModel;
        dataModel = (dataModel != null && !dataModel.isEmpty()) ? dataModel : "opencb";

        String compression = variantCommandOptions.convertVariantCommandOptions.compression;
        compression = (compression == null || compression.isEmpty()) ? "auto" :  compression.toLowerCase();

        if (!variantCommandOptions.convertVariantCommandOptions.toJson
                && !variantCommandOptions.convertVariantCommandOptions.toAvro
                && !variantCommandOptions.convertVariantCommandOptions.toProtoBuf
                && !variantCommandOptions.convertVariantCommandOptions.fromAvro) {
            variantCommandOptions.convertVariantCommandOptions.toAvro = true;
        }

        /*
         * JSON converter. Mode 'auto' set to gzip is file name ends with .gz
         */
        if (variantCommandOptions.convertVariantCommandOptions.toJson) {
            if (compression.equals("auto")) {
                if (output.endsWith(".gz")) {
                    compression = "gzip";
                } else if (output.equalsIgnoreCase("STDOUT") || output.endsWith("json")) {
                    compression = "";
                } else {
                    throw new IllegalArgumentException("Unknown compression extension for " + output);
                }
            }

            if (compression.equals("gzip")) {
                outputStream = new GZIPOutputStream(outputStream);
            }
            convertToJson(inputPath, dataModel, outputStream);
        }

        /*
         * Protocol Buffer 3 converter. Mode 'auto' set to gzip is file name ends with .gz
         */
        if (variantCommandOptions.convertVariantCommandOptions.toProtoBuf) {
            if (compression.equals("auto")) {
                if (output.endsWith(".gz")) {
                    compression = "gzip";
                } else if (output.equalsIgnoreCase("STDOUT")
                        || output.endsWith("pb")
                        || output.endsWith("pb3")
                        || output.endsWith("proto")) {
                    compression = "";
                } else {
                    throw new IllegalArgumentException("Unknown compression extension for " + output);
                }
            }

            if (compression.equals("gzip")) {
                outputStream = new GZIPOutputStream(outputStream);
            }
            convertToProtoBuf(inputPath, outputStream);
        }

        /*
         * Avro converter. Mode 'auto' set to gzip is file name ends with .gz
         */
        if (variantCommandOptions.convertVariantCommandOptions.toAvro) {
            // if compression mode is set to 'auto' it is inferred from files extension
            if (compression.equals("auto")) {
                // if output is a defined file and contains an extension
                if (output.contains(".")) {
                    String[] split = output.split("\\.");
                    switch (split[split.length - 1]) {
                        case "gz":
                        case "deflate":
                            compression = "deflate";
                            break;
                        case "sz":
                        case "snz":
                            compression = "snappy";
                            break;
                        case "bzip2":
                            compression = "bzip2";
                            break;
                        case "xz":
                            compression = "xz";
                            break;
                        default:
                            compression = "deflate";
                            break;
                    }
                } else {    // if we reach this point is very likely output is set to STDOUT
                    compression = "deflate";
                }
            }

            convertToAvro(inputPath, compression, dataModel, outputStream);

            if (isFile) {
                String metaFile = output + ".meta";
                logger.info("Write metadata into " + metaFile);
                try (FileOutputStream out = new FileOutputStream(metaFile)) {
                    writeAvroStats(new AvroFileWriter<>(VariantFileMetadata.getClassSchema(), compression, out), output);
                }
            }
        } else {
            if (variantCommandOptions.convertVariantCommandOptions.fromAvro) {
                logger.info("NOT IMPLEMENTED YET");
            }
        }

        if (outputStream != null) {
            outputStream.flush();
            outputStream.close();
        }
    }

    private void convertToJson(Path inputPath, String dataModel, OutputStream outputStream) throws IOException {
        VCFFileReader reader = new VCFFileReader(inputPath.toFile(), false);
        switch (dataModel.toLowerCase()) {
            case "opencb": {

                VariantContextToVariantConverter variantContextToVariantConverter =
                        new VariantContextToVariantConverter("s", "f", reader.getFileHeader().getSampleNamesInOrder());

//                VariantContextToVariantConverter variantContextToVariantConverter = new VariantContextToVariantConverter("", "", null);

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
        // Creating reader
        VcfBlockIterator iterator = (StringUtils.equals("-", inputPath.toAbsolutePath().toString()))
                ? new VcfBlockIterator(new BufferedInputStream(System.in), new FullVcfCodec())
                : new VcfBlockIterator(inputPath.toFile(), new FullVcfCodec());


        LocalCliOptionsParser.ConvertVariantCommandOptions cliOptions = variantCommandOptions.convertVariantCommandOptions;
        int numTasks = Math.max(cliOptions.numThreads, 1);
        int batchSize = Integer.parseInt(cliOptions.options.getOrDefault("batch.size", "50"));
        int bufferSize = Integer.parseInt(cliOptions.options.getOrDefault("buffer.size", "100000"));
        int capacity = numTasks + 1;
        ParallelTaskRunner.Config config = new ParallelTaskRunner.Config(numTasks, batchSize, capacity, true, false);

        ParallelTaskRunner<CharSequence, ByteBuffer> runner = new ParallelTaskRunner<>(
                iterator.toLineDataReader(),
                () -> { //Task supplier. Will supply a task instance for each thread.

                    //VCFCodec is not thread safe. MUST exist one instance per thread
                    VCFCodec codec = new FullVcfCodec(iterator.getHeader(), iterator.getVersion());
                    VariantContextBlockIterator blockIterator = new VariantContextBlockIterator(codec);
                    Converter<VariantContext, VariantProto.Variant> converter = new VariantContextToVariantProtoConverter();
                    return new ProtoEncoderTask<>(charBuffer -> converter.convert(blockIterator.convert(charBuffer)), bufferSize);
                },
                batch -> {
                    batch.forEach(byteBuffer -> {
                        try {
                            outputStream.write(byteBuffer.array(), byteBuffer.arrayOffset(), byteBuffer.limit());
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
                    return true;
                }, config
        );
        runner.run();
        outputStream.close();

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

    private void convertToAvro(Path inputPath, String compression, String dataModel, OutputStream outputStream) throws Exception {
        // Creating reader
        VcfBlockIterator iterator = (StringUtils.equals("-", inputPath.toAbsolutePath().toString()))
                ? new VcfBlockIterator(new BufferedInputStream(System.in), new FullVcfCodec())
                : new VcfBlockIterator(inputPath.toFile(), new FullVcfCodec());

        DataReader<CharBuffer> vcfDataReader = iterator.toCharBufferDataReader();


        // main loop
        int numTasks = Math.max(variantCommandOptions.convertVariantCommandOptions.numThreads, 1);
        int batchSize = 1024 * 1024;  //Batch size in bytes
        int capacity = numTasks + 1;
//            VariantConverterContext variantConverterContext = new VariantConverterContext();
        ParallelTaskRunner.Config config = new ParallelTaskRunner.Config(numTasks, batchSize, capacity, false);
        ParallelTaskRunner<CharBuffer, ByteBuffer> runner;
        switch (dataModel.toLowerCase()) {
            case "opencb": {
                Schema classSchema = VariantAvro.getClassSchema();
                // Converter
                final VariantContextToVariantConverter converter = new VariantContextToVariantConverter("", "");
                // Writer
                AvroFileWriter<VariantAvro> avroFileWriter = new AvroFileWriter<>(classSchema, compression, outputStream);

                runner = new ParallelTaskRunner<>(
                        vcfDataReader,
                        () -> new VariantAvroEncoderTask<>(iterator.getHeader(), iterator.getVersion(),
                                variantContext -> converter.convert(variantContext).getImpl(), classSchema),
                        avroFileWriter, config);
                break;
            }
            case "ga4gh": {
                Schema classSchema = org.ga4gh.models.Variant.getClassSchema();
                // Converter
                final VariantContext2VariantConverter converter = new VariantContext2VariantConverter();
                converter.setVariantSetId("");  //TODO: Set VariantSetId
                // Writer
                AvroFileWriter<org.ga4gh.models.Variant> avroFileWriter = new AvroFileWriter<>(classSchema, compression, outputStream);

                runner = new ParallelTaskRunner<>(
                        vcfDataReader,
                        () -> new VariantAvroEncoderTask<>(iterator.getHeader(), iterator.getVersion(), converter, classSchema),
                        avroFileWriter, config);
                break;
            }
            default:
                throw new IllegalArgumentException("Unknown dataModel \"" + dataModel + "\"");
        }
        long start = System.currentTimeMillis();
        runner.run();

        logger.debug("Time " + (System.currentTimeMillis() - start) / 1000.0 + "s");
    }

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
            aw.writeDatum(builder.build());
        } finally {
            try {
                aw.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
