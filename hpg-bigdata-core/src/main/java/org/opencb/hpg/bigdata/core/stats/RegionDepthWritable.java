package org.opencb.hpg.bigdata.core.stats;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.ga4gh.models.CigarUnit;

public class RegionDepthWritable  implements Writable {

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
				arrayPos += cu.getOperationLength();
				break;
			default:
				break;			
			}
		}
	}
	
	public void merge(RegionDepthWritable value) {
		int offset = (int) (value.position - position);
		for (int i = 0; i < value.size; i++) {
			try {
				array[i + offset] += value.array[i];
			} catch (Exception e) {
				System.out.println("out of bounds (offset = " + offset + ")");
				System.out.println("\tdest: (chrom, pos, size) = (" + chrom + ", " + position + ", " + size + ")");
				System.out.println("\tsrc : (chrom, pos, size) = (" + value.chrom + ", " + value.position + ", " + value.size + ")");
				
				size += value.size;
				array = Arrays.copyOf(array, size);
				array[i + offset] += value.array[i];
				
				System.out.println("\t\tresizing...: i = " + i + " of " + value.size + ", (i + offset) = " + (i + offset) + " of " + size);
			}
		}				
	}
	
	public String toString(int size) {
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
