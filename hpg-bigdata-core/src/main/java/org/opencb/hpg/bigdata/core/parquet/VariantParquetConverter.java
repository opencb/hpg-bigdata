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

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeader;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.metadata.Cohort;
import org.opencb.biodata.models.metadata.SampleSetType;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.biodata.models.variant.metadata.VariantStudyMetadata;
import org.opencb.biodata.tools.variant.VariantVcfHtsjdkReader;
import org.opencb.biodata.tools.variant.VcfFileReader;
import org.opencb.biodata.tools.variant.converters.avro.VariantContextToVariantConverter;
import org.opencb.biodata.tools.variant.metadata.VariantMetadataManager;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * Created by imedina on 02/08/16.
 */
public class VariantParquetConverter extends ParquetConverter<VariantAvro> {

    private String species = null;
    private String assembly = null;
    private String datasetName = null;

    public VariantParquetConverter() {
        this(CompressionCodecName.GZIP, 128 * 1024 * 1024, 128 * 1024);
    }

    public VariantParquetConverter(CompressionCodecName compressionCodecName, int rowGroupSize, int pageSize) {
        super(compressionCodecName, rowGroupSize, pageSize);

        this.schema = VariantAvro.SCHEMA$;
    }

//    public void toParquet(InputStream inputStream, String outputFilename, boolean isAvroSource) throws IOException {
//        if (isAvroSource) {
//            toParquetFromAvro(inputStream, outputFilename);
//        } else {
//            toParquetFromVcf(inputStream, outputFilename);
//        }
//    }

    public void toParquetFromVcf(String inputFilename, String outputFilename) throws IOException {
        File inputFile = new File(inputFilename);
        String filename = inputFile.getName();

        VariantMetadataManager metadataManager;
        metadataManager = new VariantMetadataManager();

        // reader
        VcfFileReader vcfFileReader = new VcfFileReader();
        vcfFileReader.open(inputFilename);
        VCFHeader vcfHeader = vcfFileReader.getVcfHeader();

        // writer
        AvroParquetWriter parquetFileWriter =
                new AvroParquetWriter(new Path(outputFilename), schema, compressionCodecName, rowGroupSize, pageSize);
//        VariantGlobalStatsCalculator statsCalculator = new VariantGlobalStatsCalculator(vcfReader.getSource());
//        statsCalculator.pre();

        // metadata management
        VariantStudyMetadata variantDatasetMetadata = new VariantStudyMetadata();
        variantDatasetMetadata.setId(datasetName);
        metadataManager.addVariantDatasetMetadata(variantDatasetMetadata);

        Cohort cohort = new Cohort("ALL", vcfHeader.getSampleNamesInOrder(), SampleSetType.MISCELLANEOUS);
        metadataManager.addCohort(cohort, variantDatasetMetadata.getId());

        // add variant file metadata from VCF header
        metadataManager.addFile(filename, vcfHeader, variantDatasetMetadata.getId());
        metadataManager.getVariantMetadata().getStudies().get(0).setAggregatedHeader(
                metadataManager.getVariantMetadata().getStudies().get(0).getFiles().get(0).getHeader());

        // main loop
        long counter = 0;
        VariantContextToVariantConverter converter = new VariantContextToVariantConverter(datasetName, filename,
                vcfHeader.getSampleNamesInOrder());

        List<VariantContext> variantContexts = vcfFileReader.read(1000);
        while (variantContexts.size() > 0) {
            for (VariantContext vc: variantContexts) {
                Variant variant = converter.convert(vc);
                if (filter(variant.getImpl())) {
                    counter++;
                    parquetFileWriter.write(variant.getImpl());
//                    statsCalculator.updateGlobalStats(variant);
                }
            }
            variantContexts = vcfFileReader.read(1000);
        }
        System.out.println("Number of processed records: " + counter);

        // close
        vcfFileReader.close();
        parquetFileWriter.close();

        // save metadata (JSON format)
        metadataManager.save(Paths.get(outputFilename + ".meta.json"), true);
    }

    public void toParquetFromVcf(InputStream inputStream, String outputFilename) throws IOException {
        // reader
        String metaFilename = outputFilename + ".meta.json";
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
