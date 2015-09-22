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

import java.nio.file.Path;
import org.apache.commons.lang.StringUtils;
import org.ga4gh.models.Variant;
import org.opencb.commons.io.DataReader;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.commons.utils.FileUtils;
import org.opencb.hpg.bigdata.app.cli.CommandExecutor;
import org.opencb.hpg.bigdata.core.converters.FullVcfCodec;
import org.opencb.hpg.bigdata.core.converters.variation.VariantAvroEncoderTask;
import org.opencb.hpg.bigdata.core.converters.variation.VariantConverterContext;
import org.opencb.hpg.bigdata.core.io.VcfBlockIterator;
import org.opencb.hpg.bigdata.core.io.avro.AvroFileWriter;

import java.io.BufferedInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

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
        String input = variantCommandOptions.convertVariantCommandOptions.input;
        String output = variantCommandOptions.convertVariantCommandOptions.output;

        // Checking input file
        FileUtils.checkFile(Paths.get(input));

        // Two options available: toAvro and fromAvro
        if (variantCommandOptions.convertVariantCommandOptions.toAvro) {
            String compression = variantCommandOptions.convertVariantCommandOptions.compression;

            // Creating reader
            VcfBlockIterator iterator = (StringUtils.equals("-", input))
                    ? new VcfBlockIterator(new BufferedInputStream(System.in), new FullVcfCodec())
                    : new VcfBlockIterator(Paths.get(input).toFile(), new FullVcfCodec());

            DataReader<CharBuffer> vcfDataReader = new DataReader<CharBuffer>() {
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

            // Creating file writer. If 'output' parameter is passed and it is different from
            // STDOUT then a file is created if parent folder exist, otherwise STDOUT is used.
            OutputStream os;
            if (output != null && !output.isEmpty() && !output.equalsIgnoreCase("STDOUT")) {
                Path parent = Paths.get(output).toAbsolutePath().getParent();
                if (parent != null) { // null if output is a file in the current directory
                    FileUtils.checkDirectory(parent, true);
                }
                os = new FileOutputStream(output);
            } else {
                os = System.out;
            }
            AvroFileWriter<Variant> avroFileWriter = new AvroFileWriter<>(Variant.getClassSchema(), compression, os);

            // main loop
            int numTasks = Math.max(variantCommandOptions.convertVariantCommandOptions.numThtreads, 1);
            int batchSize = 1024 * 1024;  //Batch size in bytes
            int capacity = numTasks + 1;
            VariantConverterContext variantConverterContext = new VariantConverterContext();
            ParallelTaskRunner.Config config = new ParallelTaskRunner.Config(numTasks, batchSize, capacity, false);
            ParallelTaskRunner<CharBuffer, ByteBuffer> runner =
                    new ParallelTaskRunner<>(
                            vcfDataReader,
                            () -> new VariantAvroEncoderTask(variantConverterContext, iterator.getHeader(), iterator.getVersion()),
                            avroFileWriter, config);
            long start = System.currentTimeMillis();
            runner.run();
            logger.debug("Time " + (System.currentTimeMillis() - start) / 1000.0 + "s");

            // close
            iterator.close();
            avroFileWriter.close();
            os.close();
        } else {
            if (variantCommandOptions.convertVariantCommandOptions.fromAvro) {
                logger.info("NOT IMPLEMENTED YET");
            }
        }
    }

}
