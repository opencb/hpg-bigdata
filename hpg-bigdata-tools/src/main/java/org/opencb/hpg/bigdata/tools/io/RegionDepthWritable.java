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

package org.opencb.hpg.bigdata.tools.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.ga4gh.models.CigarUnit;
import org.opencb.hpg.bigdata.core.stats.RegionDepth;

public class RegionDepthWritable extends RegionDepth implements Writable {

    public RegionDepthWritable(String chrom, long pos, long chunk, int size) {
        super(chrom, pos, chunk, size);
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
