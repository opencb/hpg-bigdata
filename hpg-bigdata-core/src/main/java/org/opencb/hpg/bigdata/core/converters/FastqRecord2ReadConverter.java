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

package org.opencb.hpg.bigdata.core.converters;

import htsjdk.samtools.fastq.FastqRecord;
import org.opencb.biodata.models.sequence.Read;

@Deprecated
public class FastqRecord2ReadConverter implements Converter<FastqRecord, Read> {

    @Override
    public Read forward(FastqRecord obj) {
        return new Read(obj.getReadHeader(), obj.getReadString(), obj.getBaseQualityString());
    }

    @Override
    public FastqRecord backward(Read obj) {
        return new FastqRecord(obj.getId().toString(), obj.getSequence().toString(), "+", obj.getQuality().toString());
    }

}
