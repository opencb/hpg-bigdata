package org.opencb.hpgbigdata.core.io;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

public class AvroWriter<T> {
	
	private DatumWriter<T> datumWriter;
	private DataFileWriter<T> writer;
		
	public AvroWriter(Schema schema, CodecFactory codec, OutputStream os) throws IOException {
		datumWriter = new SpecificDatumWriter<T>();
		writer = new DataFileWriter<T>(datumWriter);
		writer.setCodec(codec);
		writer.create(schema, os);
	}
	
	public void write(T obj) throws IOException {
		writer.append(obj);
	}
	
	public void close() throws IOException {
		writer.close();
	}
}
