package org.opencb.hpg.bigdata.core.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class ReadAlignmentKey implements WritableComparable<ReadAlignmentKey> {

	private String name;
	private Long chunk;

	/**
	 * Constructor.
	 */
	public ReadAlignmentKey() { }

	public ReadAlignmentKey(String name, Long chunk) {
		this.name = name;
		this.chunk = chunk;
	}

	@Override
	public String toString() {
		return (new StringBuilder())
				.append('{')
				.append(name)
				.append(',')
				.append(chunk)
				.append('}')
				.toString();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		name = WritableUtils.readString(in);
		chunk = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, name);
		out.writeLong(chunk);
	}@Override
	
	public int compareTo(ReadAlignmentKey o) {
		int result = name.compareTo(o.name);
		if(0 == result) {
			result = chunk.compareTo(o.chunk);
		}
		return result;
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Long getChunk() {
		return chunk;
	}

	public void setPos(Long chunk) {
		this.chunk = chunk;
	}
}

