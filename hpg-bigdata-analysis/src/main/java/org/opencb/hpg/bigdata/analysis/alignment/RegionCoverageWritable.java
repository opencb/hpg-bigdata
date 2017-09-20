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

package org.opencb.hpg.bigdata.analysis.alignment;

import org.apache.hadoop.io.Writable;
import org.opencb.biodata.models.alignment.RegionCoverage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Deprecated
public class RegionCoverageWritable implements Writable {

    private RegionCoverage regionCoverage;

    public RegionCoverageWritable() {
    }

    public RegionCoverageWritable(RegionCoverage regionCoverage) {
        setRegionCoverage(regionCoverage);
    }

    public RegionCoverage getRegionCoverage() {
        return regionCoverage;
    }

    public void setRegionCoverage(RegionCoverage regionCoverage) {
        this.regionCoverage = regionCoverage;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(regionCoverage.getChromosome());
        out.writeInt(regionCoverage.getStart());
//        out.writeInt(regionCoverage.chunk);
//        out.writeInt(regionCoverage.size);
        for (int i = 0; i < regionCoverage.getValues().length; i++) {
            out.writeShort(regionCoverage.getValues()[i]);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        regionCoverage = new RegionCoverage();

        regionCoverage.setChromosome(in.readUTF());
        regionCoverage.setStart(in.readInt());
//        regionCoverage.chunk = in.readInt();
//        regionCoverage.size = in.readInt();
//        regionCoverage.setValues(regionCoverage.size > 0 ? new short[regionCoverage.size] : null);
//        for (int i = 0; i < regionCoverage.size; i++) {
//            regionCoverage.array[i] = in.readShort();
//        }
    }
}
