package org.opencb.hpg.bigdata.core.stats;

import org.ga4gh.models.CigarUnit;

import java.util.Arrays;
import java.util.List;

/**
 * Created by hpccoll1 on 15/05/15.
 */
public class RegionDepth {

    public final static int CHUNK_SIZE = 4000;

    public String chrom;
    public long position;
    public long chunk;
    public int size;
    public short[] array;

    public RegionDepth() {
    }

    public RegionDepth(String chrom, long pos, long chunk, int size) {
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

    public void merge(RegionDepth value) {
        mergeChunk(value, value.chunk);
    }

    public void mergeChunk(RegionDepth value, long chunk) {

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




}
