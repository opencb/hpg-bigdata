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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.ga4gh.models.Variant;
import org.opencb.commons.io.DataReader;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.hpg.bigdata.app.cli.CommandExecutor;
import org.opencb.hpg.bigdata.core.converters.FullVcfCodec;
import org.opencb.hpg.bigdata.core.converters.variation.VariantAvroEncoderTask;
import org.opencb.hpg.bigdata.core.converters.variation.VariantConverterContext;
import org.opencb.hpg.bigdata.core.io.VcfBlockIterator;
import org.opencb.hpg.bigdata.core.io.avro.AvroFileWriter;
import org.opencb.hpg.bigdata.core.utils.PathUtils;
import org.opencb.hpg.bigdata.tools.converters.mr.Vcf2AvroMR;
import org.opencb.hpg.bigdata.tools.io.parquet.ParquetConverter;
import org.opencb.hpg.bigdata.tools.io.parquet.ParquetMR;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

/**
 * Created by imedina on 25/06/15.
 */
public class VariantCommandExecutor extends CommandExecutor {

    private CliOptionsParser.VariantCommandOptions variantCommandOptions;

    public VariantCommandExecutor(CliOptionsParser.VariantCommandOptions variantCommandOptions) {
//		super(fastqCommandOptions.logLevel, fastqCommandOptions.verbose, fastqCommandOptions.conf);

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
        }
    }

    private void convert() throws Exception {
        String input = variantCommandOptions.convertVariantCommandOptions.input;
        String output = variantCommandOptions.convertVariantCommandOptions.output;
        String compression = variantCommandOptions.convertVariantCommandOptions.compression;

        if (output == null) {
            output = input;
        }

        // clean paths
        String in = PathUtils.clean(input);
        String out = PathUtils.clean(output);

        if (variantCommandOptions.convertVariantCommandOptions.toParquet) {
            logger.info("Transform {} to parquet", input);

            if (PathUtils.isHdfs(input)) {
                new ParquetMR(Variant.getClassSchema()).run(in, out, compression);
            } else {
                new ParquetConverter<Variant>(Variant.getClassSchema()).toParquet(new FileInputStream(in), out);
            }

        } else {
            if (PathUtils.isHdfs(input)) {

                try {
                    Vcf2AvroMR.run(in, out, compression);
                } catch (Exception e) {
                    e.printStackTrace();
                }

            } else {
                // reader
                VcfBlockIterator iterator =
                        StringUtils.equals("-", in) ?
                                new VcfBlockIterator(new BufferedInputStream(System.in), new FullVcfCodec())
                                : new VcfBlockIterator(Paths.get(in).toFile(), new FullVcfCodec());
                DataReader<CharBuffer> reader = new DataReader<CharBuffer>() {
                    @Override
                    public List<CharBuffer> read(int size) {
                        return (iterator.hasNext() ? iterator.next(size) : Collections.<CharBuffer>emptyList());
                    }

                    @Override
                    public boolean close() {
                        try {
                            iterator.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                            return false;
                        }
                        return true;
                    }
                };

                // writer
                OutputStream os;
                if (PathUtils.isHdfs(output)) {
                    Configuration config = new Configuration();
                    FileSystem hdfs = FileSystem.get(config);
                    os = hdfs.create(new Path(out));
                } else {
                    os = new FileOutputStream(out);
                }

                AvroFileWriter<Variant> writer = new AvroFileWriter<>(Variant.getClassSchema(), compression, os);

                // main loop
                int numTasks = Integer.getInteger("convert.vcf2avro.parallel", 4);
                int batchSize = 1024 * 1024;  //Batch size in bytes
                int capacity = numTasks + 1;
                VariantConverterContext variantConverterContext = new VariantConverterContext();
                ParallelTaskRunner.Config config = new ParallelTaskRunner.Config(numTasks, batchSize, capacity, false);
                ParallelTaskRunner<CharBuffer, ByteBuffer> runner =
                        new ParallelTaskRunner<>(
                                reader,
                                () -> new VariantAvroEncoderTask(variantConverterContext, iterator.getHeader(), iterator.getVersion()),
                                writer, config);
                long start = System.currentTimeMillis();
                runner.run();
                System.out.println("Time " + (System.currentTimeMillis() - start) / 1000.0 + "s");

                // close
                iterator.close();
                writer.close();
                os.close();

            }
        }
    }

}
