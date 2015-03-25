package org.opencb.hpg.bigdata.core.utils.files;

import htsjdk.samtools.fastq.FastqReader;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.file.CodecFactory;
import org.opencb.ga4gh.models.Read;
import org.opencb.hpg.bigdata.core.converters.FastqRecord2ReadConverter;
import org.opencb.hpg.bigdata.core.io.AvroWriter;

public class Fastq2Ga {

	public static void convert(FastqReader reader, OutputStream os,
								 CodecFactory codec) throws IOException {
		// writer
		AvroWriter<Read> writer = new AvroWriter<>(Read.getClassSchema(), codec, os);

		// main loop
		FastqRecord2ReadConverter converter = new FastqRecord2ReadConverter();
		while (reader.hasNext()) {
			writer.write(converter.forward(reader.next()));
		}
		
		// close
		writer.close();
	}

}
