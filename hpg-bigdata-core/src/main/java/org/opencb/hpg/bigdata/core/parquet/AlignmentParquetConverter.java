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

package org.opencb.hpg.bigdata.core.parquet;

import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.ga4gh.models.ReadAlignment;


/**
 * Created by imedina on 02/08/16.
 */
public class AlignmentParquetConverter extends ParquetConverter<ReadAlignment> {

    public AlignmentParquetConverter() {
        this(CompressionCodecName.GZIP, ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE);
    }

    public AlignmentParquetConverter(CompressionCodecName compressionCodecName, int rowGroupSize, int pageSize) {
        super(compressionCodecName, rowGroupSize, pageSize);

        this.schema = ReadAlignment.SCHEMA$;
    }

    public AlignmentParquetConverter addQualityFilter(int quality) {
        addFilter(a -> !a.getImproperPlacement() && a.getAlignment().getMappingQuality() >= quality);
        return this;
    }

}
