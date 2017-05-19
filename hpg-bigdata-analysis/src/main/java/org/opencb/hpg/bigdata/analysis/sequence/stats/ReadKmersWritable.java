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
import org.opencb.biodata.tools.alignment.stats.SequenceKmers;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

public class ReadKmersWritable implements Writable {

    private SequenceKmers kmers;

    public ReadKmersWritable() {
    }

    public ReadKmersWritable(SequenceKmers kmers) {
        setKmers(kmers);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(kmers.kvalue);
        out.writeInt(kmers.kmersMap.size());
        for (String key:kmers.kmersMap.keySet()) {
            out.writeUTF(key);
            out.writeInt(kmers.kmersMap.get(key));
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        kmers = new SequenceKmers(in.readInt());
        int size = in.readInt();
        kmers.kmersMap = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            kmers.kmersMap.put(in.readUTF(), in.readInt());
        }
    }

    public SequenceKmers getKmers() {
        return kmers;
    }

    public void setKmers(SequenceKmers kmers) {
        this.kmers = kmers;
    }

}
