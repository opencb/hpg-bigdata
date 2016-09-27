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

import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.biodata.tools.variant.VariantVcfHtsjdkReader;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * Created by imedina on 02/08/16.
 */
public class VariantParquetConverter extends ParquetConverter<VariantAvro> {

    public VariantParquetConverter() {
        this(CompressionCodecName.GZIP, 128 * 1024 * 1024, 128 * 1024);
    }

    public VariantParquetConverter(CompressionCodecName compressionCodecName, int rowGroupSize, int pageSize) {
        super(compressionCodecName, rowGroupSize, pageSize);

        this.schema = VariantAvro.SCHEMA$;
    }

    public void toParquet(InputStream inputStream, String outputFilename, boolean isAvroSource) throws IOException {
        if (isAvroSource) {
            toParquetFromAvro(inputStream, outputFilename);
        } else {
            toParquetFromVcf(inputStream, outputFilename);
        }
    }

    public void toParquetFromVcf(InputStream inputStream, String outputFilename) throws IOException {
        // reader
        String metaFilename = outputFilename + ".meta";
        VariantVcfHtsjdkReader vcfReader = new VariantVcfHtsjdkReader(inputStream, null);
        vcfReader.open();
        vcfReader.pre();

        // writer
        AvroParquetWriter parquetFileWriter =
                new AvroParquetWriter(new Path(outputFilename), schema, compressionCodecName, rowGroupSize, pageSize);

        // init stats
        //VariantGlobalStatsCalculator statsCalculator = new VariantGlobalStatsCalculator(vcfReader.getSource());
        //statsCalculator.pre();

        // main loop
        long counter = 0;
        List<Variant> variants;
        while (true) {
            variants = vcfReader.read(1000);
            if (variants.size() == 0) {
                break;
            }
            // write variants and update stats
            for (Variant variant: variants) {
                if (filter(variant.getImpl())) {
                    counter++;
                    parquetFileWriter.write(variant.getImpl());
                    //statsCalculator.updateGlobalStats(variant);
                }
            }
        }
        System.out.println("Number of processed records: " + counter);

        // close
        vcfReader.post();
        vcfReader.close();
        parquetFileWriter.close();
    }

    public VariantParquetConverter addRegionFilter(Region region) {
        addFilter(v -> v.getChromosome().equals(region.getChromosome())
                && v.getEnd() >= region.getStart()
                && v.getStart() <= region.getEnd());
        return this;
    }

    public VariantParquetConverter addRegionFilter(List<Region> regions, boolean and) {
        List<Predicate<VariantAvro>> predicates = new ArrayList<>();
        regions.forEach(r -> predicates.add(v -> v.getChromosome().equals(r.getChromosome())
                && v.getEnd() >= r.getStart()
                && v.getStart() <= r.getEnd()));
        addFilter(predicates, and);
        return this;
    }

    public VariantParquetConverter addValidIdFilter() {
        addFilter(v -> v.getId() != null && !v.getId().isEmpty() && !v.getId().equals("."));
        return this;
    }

}
