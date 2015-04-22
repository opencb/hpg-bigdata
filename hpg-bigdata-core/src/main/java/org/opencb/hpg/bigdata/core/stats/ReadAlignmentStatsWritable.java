package org.opencb.hpg.bigdata.core.stats;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class ReadAlignmentStatsWritable extends ReadAlignmentStats implements Writable {
	
	public ReadAlignmentStatsWritable()  {
		super();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		ReadStatsWritable aux = new ReadStatsWritable(readStats);
		aux.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		ReadStatsWritable aux = new ReadStatsWritable();
		aux.readFields(in);
		readStats.set(aux);
	}
}
