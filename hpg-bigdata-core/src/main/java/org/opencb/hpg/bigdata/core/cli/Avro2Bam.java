package org.opencb.hpg.bigdata.core.cli;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.PrintWriter;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.ga4gh.models.ReadAlignment;
import org.opencb.ga4gh.utils.ReadAlignmentUtils;

// command lines:
//
// mvn install && java -classpath hpg-bigdata-core/target/hpg-bigdata-core-0.1.0-jar-with-dependencies.jar org.opencb.hpgbigdata.core.cli.Avro2Fastq /home/jtarraga/tests/hpg-bigdata/5.fq.avro /home/jtarraga/tests/hpg-bigdata/5.fq.avro.fq
// mvn install && hadoop jar hpg-bigdata-core/target/hpg-bigdata-core-0.1.0-jar-with-dependencies.jar org.opencb.hpgbigdata.core.cli.Avro2Fastq 5.fq.avro /home/jtarraga/tests/hpg-bigdata/5.fq.avro.hadoop.fq --hadoop

public class Avro2Bam {

	public static void main(String[] args) throws Exception {
		// tmp check parameters
		// we should use something more sophisticated as JCommander 
		if (args.length < 2) {
			System.out.println("Error: Mismatch parameters");
			System.out.println("Usage: avro2sam <source> <destination> [--hadoop]");
			System.exit(-1);
		}
		String src = args[0];
		String dest = args[1];
		boolean hadoop = false;
		if (args.length > 2) {
			hadoop = ("--hadoop".equalsIgnoreCase(args[2]));
			if (!hadoop) {
				System.out.println("Error: Unknown parameter " + args[2]);
				System.out.println("Usage: avro2sam <source> <destination> [--hadoop]");
				System.exit(-1);
			}
		}

		System.out.println("Executing Avro to SAM: from " + src + " to " + dest);

		InputStream is = null;
		
		// check hadoop
		if (hadoop) {	
			Configuration config = new Configuration();
			FileSystem hdfs = FileSystem.get(config);

			is = hdfs.open(new Path(src));
		} else {
			is = new FileInputStream(src);
		}
		
		// reader
		DataFileStream<ReadAlignment> reader = new DataFileStream<ReadAlignment>(is, new SpecificDatumReader<ReadAlignment>(ReadAlignment.class));

		// writer
		PrintWriter writer = new PrintWriter(new FileWriter(dest));
		
		for (ReadAlignment readAlignment: reader) {
			writer.write(ReadAlignmentUtils.getSamString(readAlignment));
		}
		
		// close
		reader.close();
		writer.close();
		is.close();
		
		System.out.println("Done !");
	}
}
