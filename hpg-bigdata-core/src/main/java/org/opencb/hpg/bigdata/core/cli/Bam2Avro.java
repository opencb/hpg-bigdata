package org.opencb.hpg.bigdata.core.cli;

import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;

import org.apache.avro.file.CodecFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.ga4gh.models.ReadAlignment;
import org.opencb.ga4gh.utils.ReadAlignmentUtils;
import org.opencb.hpg.bigdata.core.converters.SAMRecord2ReadAlignmentConverter;
import org.opencb.hpg.bigdata.core.io.AvroWriter;

public class Bam2Avro {

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

//		final SAMFileWriter outputSam = new SAMFileWriterFactory().makeSAMOrBAMWriter(reader.getFileHeader(),
//		        true, outputSamOrBamFile);
//		outputSam.addAlignment(samRecord);
//		outputSam.close();

		// converter
		SAMRecord2ReadAlignmentConverter converter = new SAMRecord2ReadAlignmentConverter();
		
		// writer
		AvroWriter<ReadAlignment> writer = new AvroWriter<ReadAlignment>(ReadAlignment.getClassSchema(), CodecFactory.snappyCodec(), os);
		
		// reader
		SamReader reader = SamReaderFactory.makeDefault().open(new File(src)); 
		
		for (final SAMRecord samRecord : reader) {
			writer.write(converter.forward(samRecord));
        }
		
		// close
		reader.close();
		writer.close();
		os.close();
		
		System.out.println("Done !");
	}
}


