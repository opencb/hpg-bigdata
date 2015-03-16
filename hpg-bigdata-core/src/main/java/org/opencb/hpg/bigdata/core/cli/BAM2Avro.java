package org.opencb.hpg.bigdata.core.cli;

import java.io.FileOutputStream;
import java.io.OutputStream;

import org.apache.avro.file.CodecFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.ga4gh.models.ReadAlignment;
import org.opencb.hpg.bigdata.core.converters.FastqRecord2ReadConverter;
import org.opencb.hpg.bigdata.core.io.AvroWriter;


public class BAM2Avro {

	public static void main(String[] args) throws Exception {
		// tmp check parameters
		// we should use something more sophisticated as JCommander 
		if (args.length < 2) {
			System.out.println("Error: Mismatch parameters");
			System.out.println("Usage: bam2avro <source> <destination> [--hadoop]");
			System.exit(-1);
		}
		String src = args[0];
		String dest = args[1];
		boolean hadoop = false;
		if (args.length > 2) {
			hadoop = ("--hadoop".equalsIgnoreCase(args[2]));
			if (!hadoop) {
				System.out.println("Error: Unknown parameter " + args[2]);
				System.out.println("Usage: bam2avro <source> <destination> [--hadoop]");
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
		AvroWriter<ReadAlignment> writer = new AvroWriter<ReadAlignment>(ReadAlignment.getClassSchema(), CodecFactory.snappyCodec(), os);
		
		// reader
		FastqRecord2ReadConverter converter = new FastqRecord2ReadConverter();
/*		
		BAMFileReader bamReader = new BAMFileReader(new File(src));  
		
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
*/		
		System.out.println("Done !");
	}
}
