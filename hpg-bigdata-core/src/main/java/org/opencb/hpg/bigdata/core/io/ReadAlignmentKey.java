package org.opencb.hpg.bigdata.core.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class ReadAlignmentKey implements WritableComparable<ReadAlignmentKey> {

	private String chrom;
	private Long pos;

	/**
	 * Constructor.
	 */
	public ReadAlignmentKey() { }

	public ReadAlignmentKey(String chrom, Long pos) {
		this.chrom = chrom;
		this.pos = pos;
	}

	@Override
	public String toString() {
		return (new StringBuilder())
				.append('{')
				.append(chrom)
				.append(',')
				.append(pos)
				.append('}')
				.toString();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		chrom = WritableUtils.readString(in);
		pos = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, chrom);
		out.writeLong(pos);
	}@Override
	
	public int compareTo(ReadAlignmentKey o) {
		int result = chrom.compareTo(o.chrom);
		if(0 == result) {
			result = pos.compareTo(o.pos);
		}
		return result;
	}
	
	public String getChrom() {
		return chrom;
	}

	public void setChrom(String chrom) {
		this.chrom = chrom;
	}

	public Long getPos() {
		return pos;
	}

	public void setPos(Long pos) {
		this.pos = pos;
	}
}

