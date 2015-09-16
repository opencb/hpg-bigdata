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

package org.opencb.hpg.bigdata.tools.alignment;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.opencb.biodata.tools.alignment.tasks.RegionDepth;

public class RegionDepthWritable implements Writable {

	public RegionDepth regionDepth;

	public RegionDepthWritable() { }

	public RegionDepthWritable(RegionDepth regionDepth) { setRegionDepth(regionDepth); }

	public RegionDepth getRegionDepth() {
		return regionDepth;
	}

	public void setRegionDepth(RegionDepth regionDepth) {
		this.regionDepth = regionDepth;
	}

    @Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(regionDepth.chrom);
		out.writeLong(regionDepth.position);
		out.writeLong(regionDepth.chunk);
		out.writeInt(regionDepth.size);
		for (int i = 0; i < regionDepth.size; i++) {
			out.writeShort(regionDepth.array[i]);
		}		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		regionDepth = new RegionDepth();

		regionDepth.chrom = in.readUTF();
		regionDepth.position = in.readLong();
		regionDepth.chunk = in.readLong();
		regionDepth.size = in.readInt();
		regionDepth.array = (regionDepth.size > 0 ? new short[regionDepth.size] : null);
		for (int i = 0; i < regionDepth.size; i++) {
			regionDepth.array[i] = in.readShort();
		}		
	}
}
