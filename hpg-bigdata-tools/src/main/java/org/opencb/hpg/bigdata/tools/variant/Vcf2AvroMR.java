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

package org.opencb.hpg.bigdata.tools.variant;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeader;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.ga4gh.models.CallSet;
import org.ga4gh.models.Variant;
import org.ga4gh.models.VariantSet;
import org.opencb.biodata.tools.alignment.tasks.RegionDepth;
import org.opencb.hpg.bigdata.core.converters.FullVcfCodec;
import org.opencb.hpg.bigdata.core.converters.variation.Genotype2CallSet;
import org.opencb.hpg.bigdata.core.converters.variation.VariantContext2VariantConverter;
import org.opencb.hpg.bigdata.core.converters.variation.VariantConverterContext;
import org.opencb.hpg.bigdata.core.io.VcfBlockIterator;
import org.opencb.hpg.bigdata.tools.utils.ChunkKey;
import org.opencb.hpg.bigdata.tools.utils.CompressionUtils;
import org.seqdoop.hadoop_bam.VCFInputFormat;
import org.seqdoop.hadoop_bam.VariantContextWritable;

import java.io.*;
import java.util.List;

/**
 * Created by hpccoll1 on 18/05/15.
 */
public class Vcf2AvroMR {

    public static final String VARIANT_HEADER = "variantHeader";

    public static class VariantWritable extends Variant implements Writable {

        @Override
        public void write(DataOutput dataOutput) throws IOException {

        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {

        }
    }

    private static VariantConverterContext variantConverterContext;

    public static class Vcf2AvroMapper extends Mapper<LongWritable, VariantContextWritable, ChunkKey, AvroValue<Variant>> {

        private VariantContext2VariantConverter converter = new VariantContext2VariantConverter();
        private VCFHeader header;

        @Override
        protected void setup(Mapper.Context context) throws IOException, InterruptedException {

            byte[] variantHeaders = context.getConfiguration().get(VARIANT_HEADER).getBytes();
            VcfBlockIterator iterator = new VcfBlockIterator(new ByteArrayInputStream(variantHeaders), new FullVcfCodec());
            header = iterator.getHeader();


            int gtSize = header.getGenotypeSamples().size();

            variantConverterContext = new VariantConverterContext();

            VariantSet vs = new VariantSet();
//        vs.setId(file.getName());
//        vs.setDatasetId(file.getName());
//        vs.setReferenceSetId("test");
            vs.setId("test"); //TODO
            vs.setDatasetId("test");
            vs.setReferenceSetId("test");

            List<String> genotypeSamples = header.getGenotypeSamples();
            Genotype2CallSet gtConverter = new Genotype2CallSet();
            for(int gtPos = 0; gtPos < gtSize; ++gtPos){
                CallSet cs = gtConverter.forward(genotypeSamples.get(gtPos));
                cs.getVariantSetIds().add(vs.getId());
                variantConverterContext.getCallSetMap().put(cs.getName(), cs);
//                callWriter.write(cs);
            }

            converter.setContext(variantConverterContext);
        }


        @Override
        public void map(LongWritable key, VariantContextWritable value, Context context) throws IOException, InterruptedException {
            ChunkKey newKey;

            VariantContext variantContext = value.get();
            long start_chunk = variantContext.getStart() / RegionDepth.CHUNK_SIZE;
            long end_chunk = variantContext.getEnd() / RegionDepth.CHUNK_SIZE;
            newKey = new ChunkKey(variantContext.getContig(), start_chunk);
            Variant variant = converter.forward(variantContext);
            context.write(newKey, new AvroValue<>(variant));
        }
    }

    public static class Vcf2AvroReducer extends Reducer<ChunkKey, AvroValue<Variant>, AvroKey<Variant>, NullWritable> {
        @Override
        public void reduce(ChunkKey key, Iterable<AvroValue<Variant>> values, Context context) throws IOException, InterruptedException {
            for (AvroValue<Variant> value : values) {
                context.write(new AvroKey<>(value.datum()), NullWritable.get());
            }
        }
    }

    public static int run(String input, String output, String codecName) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        int size = 500000;
        byte[] bytes = new byte[size];

        InputStream inputStream = fs.open(new Path(input));
        if (input.endsWith(".gz")) {
            throw new UnsupportedOperationException("Unable to read gzip vcf");
        }
        inputStream.read(bytes, 0, size);
        conf.set(VARIANT_HEADER, new String(bytes));

        Job job = Job.getInstance(conf, "Vcf2AvroMR");
        job.setJarByClass(Vcf2AvroMR.class);

        // Avro problem fix
        job.getConfiguration().set("mapreduce.job.user.classpath.first", "true");

        // We call setOutputSchema first so we can override the configuration
        // parameters it sets
        AvroJob.setOutputKeySchema(job, Variant.getClassSchema());
        job.setOutputValueClass(NullWritable.class);
        AvroJob.setMapOutputValueSchema(job, Variant.getClassSchema());

        // point to input data
        FileInputFormat.setInputPaths(job, new Path(input));
        job.setInputFormatClass(VCFInputFormat.class);

        // set the output format
        FileOutputFormat.setOutputPath(job, new Path(output));
        if (codecName != null) {
            FileOutputFormat.setCompressOutput(job, true);
            FileOutputFormat.setOutputCompressorClass(job, CompressionUtils.getHadoopCodec(codecName));
        }
        job.setOutputFormatClass(AvroKeyOutputFormat.class);

        job.setMapOutputKeyClass(ChunkKey.class);
        job.setMapOutputValueClass(AvroValue.class);


/*
		job.setOutputFormatClass(AvroParquetOutputFormat.class);
		AvroParquetOutputFormat.setOutputPath(job, outputPath);
		AvroParquetOutputFormat.setSchema(job, schema);
		AvroParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
		AvroParquetOutputFormat.setCompressOutput(job, true);

		// set a large block size to ensure a single row group.  see discussion
		AvroParquetOutputFormat.setBlockSize(job, 500 * 1024 * 1024);
*/

        job.setMapperClass(Vcf2AvroMapper.class);
        job.setReducerClass(Vcf2AvroReducer.class);

        return (job.waitForCompletion(true) ? 0 : 1);
    }
}
