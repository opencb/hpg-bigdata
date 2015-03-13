package org.opencb.hpgbigdata.core.cli;

import htsjdk.samtools.fastq.FastqReader;
import htsjdk.samtools.fastq.FastqRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.ga4gh.models.Read;
import org.opencb.hpgbigdata.core.converters.FastqRecord2ReadConverter;
import org.opencb.hpgbigdata.core.io.AvroWriter;

// command lines:
//
// mvn install && java -classpath hpg-bigdata-core/target/hpg-bigdata-core-0.1.0-jar-with-dependencies.jar org.opencb.hpgbigdata.core.cli.Fastq2Avro /home/jtarraga/tests/hpg-bigdata/5.fq /home/jtarraga/tests/hpg-bigdata/5.fq.avro
// hadoop fs -rm -R 5.fq.avro ; mvn install && hadoop jar hpg-bigdata-core/target/hpg-bigdata-core-0.1.0-jar-with-dependencies.jar org.opencb.hpgbigdata.core.cli.Fastq2Avro /home/jtarraga/tests/hpg-bigdata/5.fq 5.fq.avro --hadoop

public class Fastq2Avro {

	public static void main(String[] args) throws Exception {
		// tmp check parameters
		// we should use something more sophisticated as JCommander 
		if (args.length < 2) {
			System.out.println("Error: Mismatch parameters");
			System.out.println("Usage: fastq2avro <source> <destination> [--hadoop]");
			System.exit(-1);
		}
		String src = args[0];
		String dest = args[1];
		boolean hadoop = false;
		if (args.length > 2) {
			hadoop = ("--hadoop".equalsIgnoreCase(args[2]));
			if (!hadoop) {
				System.out.println("Error: Unknown parameter " + args[2]);
				System.out.println("Usage: fastq2avro <source> <destination> [--hadoop]");
				System.exit(-1);
			}
		}

		System.out.println("Executing Fastq to Avro: from " + src + " to " + dest);

		OutputStream os = null;
		
		// check hadoop
		if (hadoop) {	
			Configuration config = new Configuration();
			FileSystem hdfs = FileSystem.get(config);

			os = hdfs.create(new Path(dest));
		} else {
			os = new FileOutputStream(dest);
		}
		
		// writer
		AvroWriter<Read> writer = new AvroWriter<Read>(Read.getClassSchema(), CodecFactory.snappyCodec(), os);
		
		// reader
		FastqRecord2ReadConverter converter = new FastqRecord2ReadConverter();
		FastqReader fqReader = new FastqReader(new File(src));
		
		// read and write loop
		while (fqReader.hasNext()) {
			FastqRecord fqRecord = fqReader.next();
			Read read = converter.forward(fqRecord);
			writer.write(read);
		}
		
		// close
		fqReader.close();
		writer.close();
		os.close();
		
		System.out.println("Done !");
	}
}
