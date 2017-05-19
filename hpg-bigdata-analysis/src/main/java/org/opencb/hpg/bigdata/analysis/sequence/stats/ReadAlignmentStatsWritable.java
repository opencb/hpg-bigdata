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

package org.opencb.hpg.bigdata.analysis.sequence.stats;

import org.apache.hadoop.io.Writable;
import org.opencb.biodata.tools.alignment.stats.AlignmentGlobalStats;
import org.opencb.biodata.tools.alignment.stats.SequenceStats;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

public class ReadAlignmentStatsWritable implements Writable {

    private AlignmentGlobalStats stats;

    public ReadAlignmentStatsWritable() {
    }

    public ReadAlignmentStatsWritable(AlignmentGlobalStats stats) {
        setStats(stats);
    }

    public AlignmentGlobalStats getStats() {
        return stats;
    }

    public void setStats(AlignmentGlobalStats stats) {
        this.stats = stats;
    }

    public void setSeqStats(SequenceStats seqStats) {
        this.stats.seqStats = seqStats;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(stats.numMapped);
        out.writeInt(stats.numUnmapped);
        out.writeInt(stats.numPaired);
        out.writeInt(stats.numMappedFirst);
        out.writeInt(stats.numMappedSecond);

        out.writeInt(stats.NM);

        out.writeInt(stats.numHardC);
        out.writeInt(stats.numSoftC);
        out.writeInt(stats.numIn);
        out.writeInt(stats.numDel);
        out.writeInt(stats.numPad);
        out.writeInt(stats.numSkip);

        out.writeInt(stats.accMappingQuality);
        out.writeInt(stats.mappingQualityMap.size());
        for (int key : stats.mappingQualityMap.keySet()) {
            out.writeInt(key);
            out.writeInt(stats.mappingQualityMap.get(key));
        }

        out.writeInt(stats.accInsert);
        out.writeInt(stats.insertMap.size());
        for (int key : stats.insertMap.keySet()) {
            out.writeInt(key);
            out.writeInt(stats.insertMap.get(key));
        }
/*
        out.writeLong(pos);
        out.writeInt(cigar.size());
        for(CigarUnit cu: cigar) {
            out.writeInt(cu.getOperation().ordinal());
            out.writeLong(cu.getOperationLength());
        }
*/
        ReadStatsWritable aux = new ReadStatsWritable(stats.seqStats);
        aux.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size;
        stats = new AlignmentGlobalStats();
        stats.numMapped = in.readInt();
        stats.numUnmapped = in.readInt();
        stats.numPaired = in.readInt();
        stats.numMappedFirst = in.readInt();
        stats.numMappedSecond = in.readInt();

        stats.NM = in.readInt();

        stats.numHardC = in.readInt();
        stats.numSoftC = in.readInt();
        stats.numIn = in.readInt();
        stats.numDel = in.readInt();
        stats.numPad = in.readInt();
        stats.numSkip = in.readInt();

        stats.accMappingQuality =  in.readInt();
        size = in.readInt();
        stats.mappingQualityMap = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            stats.mappingQualityMap.put(in.readInt(), in.readInt());
        }

        stats.accInsert =  in.readInt();
        size = in.readInt();
        stats.insertMap = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            stats.insertMap.put(in.readInt(), in.readInt());
        }
/*
        pos = in.readLong();
        size = in.readInt();
        cigar = new ArrayList<CigarUnit>(size);
        for(int i = 0; i < size; i++) {
            CigarUnit cu = new CigarUnit(CigarOperation.values()[in.readInt()], in.readLong(), null);
            cigar.add(cu);
        }
*/
        ReadStatsWritable aux = new ReadStatsWritable();
        aux.readFields(in);
        setSeqStats(aux.getStats());
    }
}
