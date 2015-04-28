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

package org.opencb.hpg.bigdata.core.stats;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class ReadKmersWritable extends ReadKmers implements Writable {
	
	public ReadKmersWritable() {
		super();
	}
	
	public ReadKmersWritable(ReadKmers kmers) {
		kvalue = kmers.kvalue;
		kmersMap = kmers.kmersMap;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(kvalue);
		
		out.writeInt(kmersMap.size());
		for(String key:kmersMap.keySet()) {
			out.writeUTF(key);
			out.writeInt(kmersMap.get(key));		
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		kvalue = in.readInt();
		
		int size = in.readInt();
		kmersMap = new HashMap<String, Integer>(size);
		for (int i = 0; i < size; i++) {
			kmersMap.put(in.readUTF(), in.readInt());
		}
	}
}
