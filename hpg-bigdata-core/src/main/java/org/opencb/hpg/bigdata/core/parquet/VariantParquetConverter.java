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
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.metadata.Cohort;
import org.opencb.biodata.models.metadata.SampleSetType;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.biodata.models.variant.metadata.VariantStudyMetadata;
import org.opencb.biodata.tools.variant.VcfFileReader;
import org.opencb.biodata.tools.variant.converters.avro.VariantContextToVariantConverter;
import org.opencb.biodata.tools.variant.metadata.VariantMetadataManager;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.hpg.bigdata.core.io.ConvertTask;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;

/**
 * Created by imedina on 02/08/16.
 */
public class VariantParquetConverter extends ParquetConverter<VariantAvro> {

    private final int batchSize = 100;

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

    public void toParquetFromVcf(String inputFilename, String outputFilename, boolean annotate) throws IOException {
        File inputFile = new File(inputFilename);
        String filename = inputFile.getName();

        VariantMetadataManager metadataManager;
        metadataManager = new VariantMetadataManager();

        // VCF reader
        VcfFileReader vcfFileReader = new VcfFileReader(inputFilename, true);
        vcfFileReader.open();
        VCFHeader vcfHeader = vcfFileReader.getVcfHeader();

        // Parquet writer
        DataWriter dataWriter = new ParquetFileWriter(outputFilename, schema, compressionCodecName, rowGroupSize,
                pageSize);

        // Metadata management
        VariantStudyMetadata variantDatasetMetadata = new VariantStudyMetadata();
        variantDatasetMetadata.setId(datasetName);
        metadataManager.addVariantDatasetMetadata(variantDatasetMetadata);

        Cohort cohort = new Cohort("ALL", vcfHeader.getSampleNamesInOrder(), SampleSetType.MISCELLANEOUS);
        metadataManager.addCohort(cohort, variantDatasetMetadata.getId());

        // Add variant file metadata from VCF header
        metadataManager.addFile(filename, vcfHeader, variantDatasetMetadata.getId());
        metadataManager.getVariantMetadata().getStudies().get(0).setAggregatedHeader(
                metadataManager.getVariantMetadata().getStudies().get(0).getFiles().get(0).getHeader());

        long counter = 0;
        VariantContextToVariantConverter converter = new VariantContextToVariantConverter(datasetName, filename,
                vcfHeader.getSampleNamesInOrder());

        // Main loop
        ConvertTask convertTask = new ConvertTask(converter, filters, annotate);
        List<VariantContext> variantContexts = vcfFileReader.read(batchSize);
        while (variantContexts.size() > 0) {
            List<VariantAvro> variantAvros = convertTask.apply(variantContexts);
            dataWriter.write(variantAvros);
            counter += variantAvros.size();
            variantContexts = vcfFileReader.read(batchSize);
        }
        System.out.println("Number of processed records: " + counter);

        // Close
        vcfFileReader.close();
        dataWriter.close();

        // Save metadata (JSON format)
        metadataManager.save(Paths.get(outputFilename + ".meta.json"), true);
    }

    public void toParquetFromVcf(String inputFilename, String outputFilename, boolean annotate, int numThreads)
            throws IOException {
        // Config parallel task runner
        ParallelTaskRunner.Config config = ParallelTaskRunner.Config.builder()
                .setNumTasks(numThreads)
                .setBatchSize(batchSize)
                .setSorted(true)
                .build();

        // VCF reader
        VcfFileReader vcfFileReader = new VcfFileReader(inputFilename, false);

        // Parquet writer
        DataWriter<VariantAvro> dataWriter = new ParquetFileWriter<>(outputFilename, schema, compressionCodecName, rowGroupSize,
                pageSize);

        // Create the parallel task runner
        ParallelTaskRunner<VariantContext, VariantAvro> ptr;
        try {
            // Converter
            VariantContextToVariantConverter converter = new VariantContextToVariantConverter(datasetName,
                    new File(inputFilename).getName(), vcfFileReader.getVcfHeader().getSampleNamesInOrder());
            ConvertTask convertTask = new ConvertTask(converter, filters, annotate);
            ptr = new ParallelTaskRunner<>(vcfFileReader, convertTask, dataWriter, config);
        } catch (Exception e) {
            throw new IOException("Error while creating ParallelTaskRunner", e);
        }
        try {
            ptr.run();
        } catch (ExecutionException e) {
            throw new IOException("Error while converting VCF to Avro in ParallelTaskRunner", e);
        }
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

    public String getSpecies() {
        return species;
    }

    public VariantParquetConverter setSpecies(String species) {
        this.species = species;
        return this;
    }

    public String getAssembly() {
        return assembly;
    }

    public VariantParquetConverter setAssembly(String assembly) {
        this.assembly = assembly;
        return this;
    }

    public String getDatasetName() {
        return datasetName;
    }

    public VariantParquetConverter setDatasetName(String datasetName) {
        this.datasetName = datasetName;
        return this;
    }
}
