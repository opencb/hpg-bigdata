package org.opencb.hpg.bigdata.core.utils.files;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.specific.SpecificDatumReader;
import org.opencb.ga4gh.models.Read;
import org.opencb.ga4gh.utils.ReadUtils;

public class Ga2Fastq {

	public static void convert(InputStream is, PrintWriter writer) throws IOException {
		// reader
		DataFileStream<Read> reader = new DataFileStream<Read>(is, new SpecificDatumReader<Read>(Read.class));

		// main loop
		for (Read read: reader) {
			writer.write(ReadUtils.getFastqString(read));
		}
		
		// close
		reader.close();
	}
}
