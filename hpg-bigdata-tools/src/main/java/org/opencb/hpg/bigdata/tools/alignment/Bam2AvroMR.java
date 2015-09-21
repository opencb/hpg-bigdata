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

package org.opencb.hpg.bigdata.tools.alignment;

import htsjdk.samtools.*;
import htsjdk.samtools.seekablestream.SeekableStream;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.ga4gh.models.ReadAlignment;
import org.opencb.hpg.bigdata.core.converters.SAMRecord2ReadAlignmentConverter;
import org.opencb.hpg.bigdata.tools.utils.ChunkKey;
import org.opencb.hpg.bigdata.tools.utils.CompressionUtils;
import org.seqdoop.hadoop_bam.AnySAMInputFormat;
import org.seqdoop.hadoop_bam.SAMRecordWritable;
import org.seqdoop.hadoop_bam.util.WrapSeekable;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class Bam2AvroMR {

    public static final String ADJUST_QUALITY = "adjustQuality";

    public static class Bam2GaMapper extends
            Mapper<LongWritable, SAMRecordWritable, AvroKey<ReadAlignment>, NullWritable> {

        private SAMRecord2ReadAlignmentConverter samRecord2ReadAlignmentConverter;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            boolean adjustQuality = context.getConfiguration().getBoolean(ADJUST_QUALITY, false);
            samRecord2ReadAlignmentConverter = new SAMRecord2ReadAlignmentConverter(adjustQuality);
        }

        @Override
        public void map(LongWritable key, SAMRecordWritable value, Context context) throws
                IOException, InterruptedException {
//          ChunkKey newKey;

            SAMRecord sam = value.get();
            if (sam.getReadUnmappedFlag()) {
                System.out.println("Empty block");
                // do nothing
                // newKey = new ChunkKey(new String("*"), (long) 0);
            } else {
//              long start_chunk = sam.getAlignmentStart() / RegionDepth.CHUNK_SIZE;
//              long end_chunk = sam.getAlignmentEnd() / RegionDepth.CHUNK_SIZE;
//              newKey = new ChunkKey(sam.getReferenceName(), start_chunk);

                SAMRecord2ReadAlignmentConverter converter = samRecord2ReadAlignmentConverter;
                ReadAlignment readAlignment = converter.forward(sam);
                AvroKey<ReadAlignment> newKey = new AvroKey<>(readAlignment);

                //context.write(newKey, value);
                context.write(newKey, NullWritable.get());
            }
        }
    }

    public static class Bam2GaReducer extends
            Reducer<ChunkKey, AvroValue<ReadAlignment>, AvroKey<ReadAlignment>, NullWritable> {

        public void reduce(ChunkKey key, Iterable<AvroValue<ReadAlignment>> values, Context context) throws
                IOException, InterruptedException {
            for (AvroValue<ReadAlignment> value : values) {
                context.write(new AvroKey<>(value.datum()), NullWritable.get());
            }
        }
    }

    public static int run(String input, String output, String codecName, boolean adjustQuality) throws Exception {
        return run(input, output, codecName, adjustQuality, new Configuration());
    }

    public static int run(String input, String output, String codecName, boolean adjQuality, Configuration conf) throws
            Exception {

        // read header, and save sequence index/name in conf
        final Path p = new Path(input);
        final SeekableStream seekableStream = WrapSeekable.openPath(conf, p);
        final SamReader reader = SamReaderFactory.make().open(SamInputResource.of(seekableStream));
        final SAMFileHeader header = reader.getFileHeader();
        int i = 0;
        SAMSequenceRecord sr;
        while ((sr = header.getSequence(i)) != null) {
            conf.set("" + i, sr.getSequenceName());
            i++;
        }

        Job job = Job.getInstance(conf, "Bam2AvroMR");
        job.setJarByClass(Bam2AvroMR.class);

        // Avro problem fix
        job.getConfiguration().set("mapreduce.job.user.classpath.first", "true");
        job.getConfiguration().set(ADJUST_QUALITY, Boolean.toString(adjQuality));

        // We call setOutputSchema first so we can override the configuration
        // parameters it sets
        AvroJob.setOutputKeySchema(job, ReadAlignment.getClassSchema());
        job.setOutputValueClass(NullWritable.class);
        AvroJob.setMapOutputValueSchema(job, ReadAlignment.getClassSchema());

        // point to input data
        FileInputFormat.setInputPaths(job, new Path(input));
        job.setInputFormatClass(AnySAMInputFormat.class);

        // set the output format
        FileOutputFormat.setOutputPath(job, new Path(output));
        if (codecName != null) {
            FileOutputFormat.setCompressOutput(job, true);
            FileOutputFormat.setOutputCompressorClass(job, CompressionUtils.getHadoopCodec(codecName));
        }
        job.setOutputFormatClass(AvroKeyOutputFormat.class);

        job.setMapOutputKeyClass(AvroKey.class);
        job.setMapOutputValueClass(Void.class);

        job.setMapperClass(Bam2GaMapper.class);
        job.setNumReduceTasks(0);

        job.waitForCompletion(true);

        // write header
        Path headerPath = new Path(output + ".header");
        FileSystem fs = FileSystem.get(conf);
        BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(headerPath, true)));
        br.write(header.getTextHeader());
        br.close();

        return 0;
    }
}
