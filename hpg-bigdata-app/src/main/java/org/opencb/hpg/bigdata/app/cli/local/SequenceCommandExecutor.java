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

import htsjdk.samtools.fastq.FastqReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.opencb.biodata.models.sequence.Read;
import org.opencb.hpg.bigdata.app.cli.CommandExecutor;
import org.opencb.hpg.bigdata.app.cli.hadoop.CliOptionsParser;
import org.opencb.hpg.bigdata.core.converters.FastqRecord2ReadConverter;
import org.opencb.hpg.bigdata.core.io.avro.AvroWriter;
import org.opencb.hpg.bigdata.core.utils.AvroUtils;
import org.opencb.hpg.bigdata.core.utils.PathUtils;
import org.opencb.hpg.bigdata.tools.converters.mr.Fastq2AvroMR;
import org.opencb.hpg.bigdata.tools.stats.read.mr.ReadKmersMR;
import org.opencb.hpg.bigdata.tools.stats.read.mr.ReadStatsMR;

import java.io.*;
import java.util.Date;

/**
 * Created by imedina on 03/02/15.
 */
public class SequenceCommandExecutor extends CommandExecutor {

	private LocalCliOptionsParser.SequenceCommandOptions sequenceCommandOptions;

	public SequenceCommandExecutor(LocalCliOptionsParser.SequenceCommandOptions sequenceCommandOptions) {
		this.sequenceCommandOptions = sequenceCommandOptions;
	}

	/**
	 * Parse specific 'sequence' command options
	 */
	public void execute() {
		String subCommand = sequenceCommandOptions.getParsedSubCommand();

        switch (subCommand) {
            case "convert":
				convert();
                break;
			case "stats":
				stats();
				break;
			default:
				break;
        }
	}

	private void convert() {
		LocalCliOptionsParser.ConvertSequenceCommandOptions convertSequenceCommandOptions = sequenceCommandOptions.convertSequenceCommandOptions;

		// get input parameters
		String input = convertSequenceCommandOptions.input;
		String output = convertSequenceCommandOptions.output;
		String codecName = convertSequenceCommandOptions.compression;

		try {
			// reader
			FastqReader reader = new FastqReader(new File(input));

			// writer
			OutputStream os = new FileOutputStream(output);

			AvroWriter<Read> writer = new AvroWriter<>(Read.getClassSchema(), AvroUtils.getCodec(codecName), os);

			// main loop
			FastqRecord2ReadConverter converter = new FastqRecord2ReadConverter();
			while (reader.hasNext()) {
				writer.write(converter.forward(reader.next()));
			}

			// close
			reader.close();
			writer.close();
			os.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
    }

	private void stats() {
		/*
		LocalCliOptionsParser.StatsSequenceCommandOptions statsSequenceCommandOptions = sequenceCommandOptions.statsSequenceCommandOptions;

		// get input parameters
		String input = statsSequenceCommandOptions.input;
		String output = statsSequenceCommandOptions.output;
		int kvalue = statsSequenceCommandOptions.kmers;

		// prepare the HDFS output folder
		FileSystem fs = null;
		Configuration conf = new Configuration();
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
		String outHdfsDirname = new String("" + new Date().getTime());

		// run MapReduce job to compute stats
		try {
			ReadStatsMR.run(input, outHdfsDirname, kvalue);
		} catch (Exception e) {
			e.printStackTrace();
		}

		// post-processing
		Path outFile = new Path(outHdfsDirname + "/part-r-00000");

		try {
			if (!fs.exists(outFile)) {
            	logger.error("Stats results file not found: {}", outFile.getName());
			} else {
				String outRawFileName =  output + "/stats.json";
				fs.copyToLocalFile(outFile, new Path(outRawFileName));

				//Utils.parseStatsFile(outRawFileName, out);
			}
			fs.delete(new Path(outHdfsDirname), true);
		} catch (IOException e) {
			e.printStackTrace();
		}
		*/
	}
}
