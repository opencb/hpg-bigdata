package org.opencb.hpg.bigdata.core.cli;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMFileWriter;
import htsjdk.samtools.SAMFileWriterFactory;
import htsjdk.samtools.SAMProgramRecord;
import htsjdk.samtools.SAMReadGroupRecord;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Map.Entry;

import org.apache.avro.file.CodecFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.ga4gh.models.ReadAlignment;
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


		// converter
		SAMRecord2ReadAlignmentConverter converter = new SAMRecord2ReadAlignmentConverter();
		
		// writer
		AvroWriter<ReadAlignment> writer = new AvroWriter<ReadAlignment>(ReadAlignment.getClassSchema(), CodecFactory.snappyCodec(), os);
		
		// reader
		SamReader reader = SamReaderFactory.makeDefault().open(new File(src));
		SAMFileHeader header = reader.getFileHeader();
		System.out.println("header:\n" + header.getTextHeader());
		
		System.out.println("sort    = " + header.getSortOrder().toString());
		System.out.println("version = " + header.getVersion().toString());
		for (Entry<String, String> attr: header.getAttributes()) {
			System.out.println(attr.getKey() + " => " + attr.getValue());
		}
		
		for (SAMProgramRecord attr: header.getProgramRecords()) {
			System.out.println("program record: " + attr.toString());
		}

		for (SAMReadGroupRecord attr: header.getReadGroups()) {
			System.out.println("read group record: " + attr.toString());
		}
		
		for (SAMSequenceRecord attr: header.getSequenceDictionary().getSequences()) {
			System.out.println("sequence record: " + attr.toString());
		}
		
		// CRAM output file
		//final CRAMFileWriter outCramWriter; // = new SAMFileWriterFactory().makeSAMOrBAMWriter(header, true, outSamFile);
		
		// SAM output file
		File outSamFile = new File("/tmp/out.sam");
		SAMFileHeader h = new SAMFileHeader();
		h.setTextHeader(header.getTextHeader());
		final SAMFileWriter outSamWriter = new SAMFileWriterFactory().makeSAMOrBAMWriter(header, true, outSamFile);
		
		// BAM output file
		File outBamFile = new File("/tmp/out.bam");
		//SAMFileHeader h = new SAMFileHeader();
		//h.setTextHeader(header.getTextHeader());
		final SAMFileWriter outBamWriter = new SAMFileWriterFactory().makeSAMOrBAMWriter(header, true, outBamFile);

		for (final SAMRecord samRecord : reader) {
			System.out.println("00 => " + samRecord.getSAMString());
			ReadAlignment ra = converter.forward(samRecord);
			System.out.println("11 => " + converter.backward(ra).getSAMString());
			System.exit(-1);
			
			writer.write(converter.forward(samRecord));
			outSamWriter.addAlignment(samRecord);
			outBamWriter.addAlignment(samRecord);
        }
		
		outSamWriter.close();
		outBamWriter.close();
		
		// close
		reader.close();
		writer.close();
		os.close();
		
		System.out.println("Done !");
	}
}


