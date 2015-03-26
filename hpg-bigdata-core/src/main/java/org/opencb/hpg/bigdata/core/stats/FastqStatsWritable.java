package org.opencb.hpg.bigdata.core.stats;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Writable;

public class FastqStatsWritable extends FastqStats implements Writable {

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(numSeqs);
		out.writeInt(numA);
		out.writeInt(numT);
		out.writeInt(numG);
		out.writeInt(numC);
		out.writeInt(numN);
		out.writeInt(minSeqLength);
		out.writeInt(maxSeqLength);
		out.writeInt(minSeqQual);
		out.writeInt(maxSeqQual);
		out.writeInt(accSeqQual);
		
		out.writeInt(lengthMap.size());
		for(int key:lengthMap.keySet()) {
			out.writeInt(key);
			out.writeInt(lengthMap.get(key));		
		}

		out.writeInt(infoMap.size());
		for(int key:infoMap.keySet()) {
			out.writeInt(key);
			final Info info = infoMap.get(key);
			out.writeInt(info.numA);
			out.writeInt(info.numT);
			out.writeInt(info.numG);
			out.writeInt(info.numC);
			out.writeInt(info.numN);
			out.writeInt(info.numQual);
			out.writeInt(info.accQual);
		}
		
		// I don't knwo why it does not work
		// new FastqKmersWritable(kmers).write(out);
		out.writeInt(kmers.kvalue);
		
		out.writeInt(kmers.kmersMap.size());
		for(String key:kmers.kmersMap.keySet()) {
			out.writeUTF(key);
			out.writeInt(kmers.kmersMap.get(key));		
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int size, key;
		Info info;

		numSeqs = in.readInt();
		numA = in.readInt();
		numT = in.readInt();
		numG = in.readInt();
		numC = in.readInt();
		numN = in.readInt();
		minSeqLength = in.readInt();
		maxSeqLength = in.readInt();
		minSeqQual = in.readInt();
		maxSeqQual = in.readInt();
		accSeqQual = in.readInt();
		
		size = in.readInt();
		lengthMap = new HashMap<Integer, Integer>(size);
		for (int i = 0; i < size; i++) {
			lengthMap.put(in.readInt(), in.readInt());
		}

		size = in.readInt();
		infoMap = new HashMap<Integer, Info>(size);
		for (int i = 0; i < size; i++) {
			key = in.readInt();
			info = new Info();
			info.numA = in.readInt();
			info.numT = in.readInt();
			info.numG = in.readInt();
			info.numC = in.readInt();
			info.numN = in.readInt();
			info.numQual = in.readInt();
			info.accQual = in.readInt();
			infoMap.put(key, info);
		}
		
		// I don't knwo why it does not work
		//new FastqKmersWritable(kmers).readFields(in);
		kmers.kvalue = in.readInt();
		
		size = in.readInt();
		kmers.kmersMap = new HashMap<String, Integer>(size);
		for (int i = 0; i < size; i++) {
			kmers.kmersMap.put(in.readUTF(), in.readInt());
		}
	}
}
