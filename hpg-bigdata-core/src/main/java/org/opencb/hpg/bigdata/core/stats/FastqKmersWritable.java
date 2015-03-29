package org.opencb.hpg.bigdata.core.stats;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class FastqKmersWritable extends FastqKmers implements Writable {
	
	public FastqKmersWritable() {
		super();
	}
	
	public FastqKmersWritable(FastqKmers kmers) {
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
