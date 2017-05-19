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

package org.opencb.hpg.bigdata.analysis.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class ChunkKey implements WritableComparable<ChunkKey> {

    private String name;
    private Integer chunk;

    /**
     * Constructor.
     */
    public ChunkKey() { }

    public ChunkKey(String name, Integer chunk) {
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
        chunk = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, name);
        out.writeInt(chunk);
    }

    @Override
     public int compareTo(ChunkKey o) {
        int result = name.compareTo(o.name);
        if (0 == result) {
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

    public Integer getChunk() {
        return chunk;
    }

    public void setPos(Integer chunk) {
        this.chunk = chunk;
    }
}
