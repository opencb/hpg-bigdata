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

package org.opencb.hpg.bigdata.tools.tasks.read.mr;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.ga4gh.models.ReadAlignment;
import org.opencb.biodata.models.sequence.Read;
import org.opencb.hpg.bigdata.core.NativeAligner;
import org.opencb.hpg.bigdata.core.converters.SAMRecord2ReadAlignmentConverter;
import org.opencb.hpg.bigdata.tools.tasks.alignment.mr.AlignerParams;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

public class ReadAlignMR {
	
	public static class ReadAlignMapper extends Mapper<AvroKey<Read>, NullWritable, AvroKey<ReadAlignment>, NullWritable> {

		String indexFolder;
		long index;

		final int MAX_NUM_AVRO_RECORDS = 40;
		ArrayList<Read> pending = new ArrayList<>(MAX_NUM_AVRO_RECORDS);

        private NativeAligner nativeAligner = new NativeAligner();

		private void mapReads(Context context) throws IOException, InterruptedException {

            StringBuilder fastq = new StringBuilder();
            for(Read read: pending) {
                fastq.append("@").append(read.getId()).append("\n");
                fastq.append(read.getSequence()).append("\n");
                fastq.append("+").append("\n");
                fastq.append(read.getQuality()).append("\n");
            }

            String sam = nativeAligner.map(fastq.toString(), index);
            System.out.println("mapReads, sam:\n" + sam);

            int lineCounter = 0;
            String lines[] = sam.split("\n");

            ReadAlignment readAlignment;
            SAMRecord2ReadAlignmentConverter converter = new SAMRecord2ReadAlignmentConverter();

            for (String line: lines) {
                readAlignment = converter.forward(converter.backward2(line));
                System.out.println(readAlignment);
                context.write(new AvroKey<>(readAlignment), NullWritable.get());
            }

			pending.clear();
		}

		public  void setup(Context context) {
			System.out.println("------> setup");

			System.out.println("Loading library hpgaligner...");
			//System.out.println("\tjava.libary.path = " + System.getProperty("java.library.path"));
            //System.out.println("\tLD_LIBRARY_PATH = " + System.getenv("LD_LIBRARY_PATH"));

            boolean loaded = false;
            String ld_library_path = System.getenv("LD_LIBRARY_PATH");
            String paths[] = ld_library_path.split(":");
            for(String path: paths) {
                if (new File(path + "/libhpgaligner.so").exists()) {
                    loaded = true;
                    System.load(path + "/libhpgaligner.so");
                    break;
                }
            }
            if (!loaded) {
                System.out.println("Library libhpgaligner.so not found. Set your environment variable: LD_LIBRARY_PATH library");
                System.exit(-1);
            }
			System.out.println("...done!");

            // load index
            Configuration conf = context.getConfiguration();
            indexFolder = conf.get("indexFolder");
            System.out.println("===========> index folder = " + indexFolder);
            index = nativeAligner.load_index(indexFolder);
		}

		public  void cleanup(Context context) {
			System.out.println("------> cleanup");
			if (pending.size() > 0) {
				try {
					mapReads(context);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

            // free index
            nativeAligner.free_index(index);
		}

		@Override
		public void map(AvroKey<Read> key, NullWritable value, Context context) throws IOException, InterruptedException {
			pending.add(key.datum());
			if (pending.size() >= MAX_NUM_AVRO_RECORDS) {
				System.out.println("------> map");
				mapReads(context);
			}
		}
	}

	public static int run(AlignerParams params) throws Exception {
		Configuration conf = new Configuration();
		conf.set("indexFolder", params.indexFolderName);

		Job job = Job.getInstance(conf, "ReadAlignMR");
		job.setJarByClass(ReadAlignMR.class);

		AvroJob.setInputKeySchema(job, Read.SCHEMA$);
		AvroJob.setOutputKeySchema(job, ReadAlignment.SCHEMA$);

		// input
		FileInputFormat.setInputPaths(job, new Path(params.seqFileName1));
		job.setInputFormatClass(AvroKeyInputFormat.class);

		// output
		FileOutputFormat.setOutputPath(job, new Path(params.resultFileName));
		job.setOutputKeyClass(AvroValue.class);
		job.setOutputValueClass(NullWritable.class);
		job.setOutputFormatClass(AvroKeyOutputFormat.class);

		// mapper
		job.setMapperClass(ReadAlignMapper.class);
		job.setMapOutputKeyClass(AvroValue.class);
		job.setMapOutputValueClass(NullWritable.class);
		AvroJob.setMapOutputKeySchema(job, ReadAlignment.SCHEMA$);

        // reducer
        job.setNumReduceTasks(0);
        
		return (job.waitForCompletion(true) ? 0 : 1);
	}
}
