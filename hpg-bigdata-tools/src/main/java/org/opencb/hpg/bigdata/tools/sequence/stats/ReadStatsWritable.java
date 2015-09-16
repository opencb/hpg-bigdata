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

package org.opencb.hpg.bigdata.tools.sequence.stats;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Writable;
import org.opencb.biodata.tools.sequence.tasks.SequenceInfo;
import org.opencb.biodata.tools.sequence.tasks.SequenceStats;

public class ReadStatsWritable implements Writable {

	public SequenceStats stats;

	public ReadStatsWritable() { }

	public ReadStatsWritable(SequenceStats stats) {	setStats(stats); }

	public SequenceStats getStats() {
		return stats;
	}

	public void setStats(SequenceStats stats) {
		this.stats = stats;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(stats.numSeqs);
		out.writeInt(stats.numA);
		out.writeInt(stats.numT);
		out.writeInt(stats.numG);
		out.writeInt(stats.numC);
		out.writeInt(stats.numN);
		out.writeInt(stats.minSeqLength);
		out.writeInt(stats.maxSeqLength);
		out.writeInt(stats.accSeqQual);
		
		out.writeInt(stats.lengthMap.size());
		for(int key:stats.lengthMap.keySet()) {
			out.writeInt(key);
			out.writeInt(stats.lengthMap.get(key));
		}

		out.writeInt(stats.infoMap.size());
		for(int key:stats.infoMap.keySet()) {
			out.writeInt(key);
			final SequenceInfo info = stats.infoMap.get(key);
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
		out.writeInt(stats.kmers.kvalue);
		
		out.writeInt(stats.kmers.kmersMap.size());
		for(String key:stats.kmers.kmersMap.keySet()) {
			out.writeUTF(key);
			out.writeInt(stats.kmers.kmersMap.get(key));
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int size, key;
		SequenceInfo info;

		stats = new SequenceStats();

		stats.numSeqs = in.readInt();
		stats.numA = in.readInt();
		stats.numT = in.readInt();
		stats.numG = in.readInt();
		stats.numC = in.readInt();
		stats.numN = in.readInt();
		stats.minSeqLength = in.readInt();
		stats.maxSeqLength = in.readInt();
		stats.accSeqQual = in.readInt();
		
		size = in.readInt();
		stats.lengthMap = new HashMap<Integer, Integer>(size);
		for (int i = 0; i < size; i++) {
			stats.lengthMap.put(in.readInt(), in.readInt());
		}

		size = in.readInt();
		stats.infoMap = new HashMap<Integer, SequenceInfo>(size);
		for (int i = 0; i < size; i++) {
			key = in.readInt();
			info = new SequenceInfo();
			info.numA = in.readInt();
			info.numT = in.readInt();
			info.numG = in.readInt();
			info.numC = in.readInt();
			info.numN = in.readInt();
			info.numQual = in.readInt();
			info.accQual = in.readInt();
			stats.infoMap.put(key, info);
		}
		
		// I don't knwo why it does not work
		//new FastqKmersWritable(kmers).readFields(in);
		stats.kmers.kvalue = in.readInt();
		
		size = in.readInt();
		stats.kmers.kmersMap = new HashMap<String, Integer>(size);
		for (int i = 0; i < size; i++) {
			stats.kmers.kmersMap.put(in.readUTF(), in.readInt());
		}
	}
}
