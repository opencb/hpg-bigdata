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

package org.opencb.hpg.bigdata.tools.tasks.alignment.mr;

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
import org.opencb.hpg.bigdata.core.AlignerParams;
import org.opencb.hpg.bigdata.core.NativeAligner;
import org.opencb.hpg.bigdata.core.converters.SAMRecord2ReadAlignmentConverter;
import org.opencb.hpg.bigdata.tools.utils.AlignerUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

public class ReadAlignmentAlignMR {
	
	public static class ReadAlignmentAlignMapper extends Mapper<AvroKey<ReadAlignment>, NullWritable, AvroKey<ReadAlignment>, NullWritable> {

		long nativeIndex;  // pointer to the Native structure for searching index
		long nativeParams; // pointer to the Native structure for aligner parameters
		AlignerParams params;

		final int MAX_NUM_AVRO_RECORDS = 40;
		ArrayList<ReadAlignment> pending = new ArrayList<>(MAX_NUM_AVRO_RECORDS);

        private NativeAligner nativeAligner = new NativeAligner();
		private AlignerUtils alignerUtils = new AlignerUtils();

		private void mapReads(Context context) throws IOException, InterruptedException {

            StringBuilder fastq = new StringBuilder();
            for(ReadAlignment readAlignment: pending) {
                fastq.append("@").append(readAlignment.getId()).append("\n");
                fastq.append(readAlignment.getAlignedSequence()).append("\n");
                fastq.append("+").append("\n");
				for(int q: readAlignment.getAlignedQuality()) {
					fastq.append((char) q);
				}
				fastq.append("\n");
            }

			alignerUtils.mapReads(fastq.toString(), nativeAligner, nativeIndex, nativeParams, context);

			pending.clear();
		}

		public  void setup(Context context) {
			System.out.println("------> setup");

			// load dynamic library
			alignerUtils.loadLibrary("libhpgaligner.so");

            // load index
            Configuration conf = context.getConfiguration();
			params = AlignerUtils.newAlignerParams(conf);
			System.out.println("params.indexFolderName = " + params.indexFolderName);
			nativeIndex = nativeAligner.load_index(params.indexFolderName);
			nativeParams = nativeAligner.load_params(params);
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

			// free
			nativeAligner.free_index(nativeIndex);
			nativeAligner.free_params(nativeParams);
		}

		@Override
		public void map(AvroKey<ReadAlignment> key, NullWritable value, Context context) throws IOException, InterruptedException {
			pending.add(key.datum());
			if (pending.size() >= MAX_NUM_AVRO_RECORDS) {
				System.out.println("------> map");
				mapReads(context);
			}
		}
	}

	public static int run(AlignerParams params) throws Exception {
		Configuration conf = new Configuration();
        conf.set("seqFileName1", params.seqFileName1);
        conf.set("seqFileName2", params.seqFileName2);
        conf.set("indexFolderName", params.indexFolderName);
        conf.set("numSeeds", "" + params.numSeeds);
        conf.set("minSWScore", "" + params.minSWScore);

		Job job = Job.getInstance(conf, "ReadAlignMR");
		job.setJarByClass(ReadAlignmentAlignMR.class);

		AvroJob.setInputKeySchema(job, ReadAlignment.SCHEMA$);
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
		job.setMapperClass(ReadAlignmentAlignMapper.class);
		job.setMapOutputKeyClass(AvroValue.class);
		job.setMapOutputValueClass(NullWritable.class);
		AvroJob.setMapOutputKeySchema(job, ReadAlignment.SCHEMA$);

        // reducer
        job.setNumReduceTasks(0);
        
		return (job.waitForCompletion(true) ? 0 : 1);
	}
}
