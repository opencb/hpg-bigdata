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

package org.opencb.hpg.bigdata.core.io.avro;

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
		writer = new DataFileWriter<>(datumWriter);
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
