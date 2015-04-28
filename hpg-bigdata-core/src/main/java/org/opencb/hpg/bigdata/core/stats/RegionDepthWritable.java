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
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.ga4gh.models.CigarUnit;

public class RegionDepthWritable  implements Writable {

	public final static int CHUNK_SIZE = 4000;


	public String chrom;
	public long position;
	public long chunk;
	public int size;
	public short[] array; 
	
	public RegionDepthWritable() {
	}
	
	public RegionDepthWritable(String chrom, long pos, long chunk, int size) {
		this.chrom = chrom;
		this.position = pos;
		this.chunk = chunk;
		this.size = size;
		this.array = (size > 0 ? new short[size] : null);
	}
	
	public void update(long pos, List<CigarUnit> cigar) {
		int arrayPos = (int) (pos - position);
		for (CigarUnit cu: cigar) {
			switch (cu.getOperation()) {
			case ALIGNMENT_MATCH:
			case SEQUENCE_MATCH:
			case SEQUENCE_MISMATCH:
				for (int i = 0; i < cu.getOperationLength(); i++) {
					array[arrayPos++]++;
				}
				break;
			case CLIP_HARD:
			case CLIP_SOFT:
			case DELETE:
			case INSERT:
			case PAD:
				break;
			case SKIP:
				// resize
				size += cu.getOperationLength();
				array = Arrays.copyOf(array, size);
				arrayPos += cu.getOperationLength();
				break;
			default:
				break;			
			}
		}
	}
	
	public void merge(RegionDepthWritable value) {
		mergeChunk(value, value.chunk);
	}
	
	public void mergeChunk(RegionDepthWritable value, long chunk) {
		
		int start = (int) Math.max(value.position, chunk * CHUNK_SIZE);
		int end = (int) Math.min(value.position + value.size - 1, (chunk + 1) * CHUNK_SIZE - 1);
		
		int srcOffset = (int) value.position;
		int destOffset = (int) (chunk * CHUNK_SIZE);
		
		for (int i = start ; i <= end; i++) {
			array[i - destOffset] += value.array[i - srcOffset];
		}
	}
	
	public String toString() {
		StringBuilder res = new StringBuilder();
		for (int i = 0; i < size; i++) {
			res.append(chrom + "\t" +  (position + i) + "\t" + array[i] + "\n");
		}
		return res.toString();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(chrom);
		out.writeLong(position);
		out.writeLong(chunk);
		out.writeInt(size);
		for (int i = 0; i < size; i++) {
			out.writeShort(array[i]);
		}		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		chrom = in.readUTF();
		position = in.readLong();
		chunk = in.readLong();
		size = in.readInt();
		array = (size > 0 ? new short[size] : null);
		for (int i = 0; i < size; i++) {
			array[i] = in.readShort();
		}		
	}
}
