package org.opencb.hpg.bigdata.core.avro;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeader;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.metadata.Cohort;
import org.opencb.biodata.models.metadata.SampleSetType;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.biodata.models.variant.metadata.VariantStudyMetadata;
import org.opencb.biodata.tools.variant.VcfFileReader;
import org.opencb.biodata.tools.variant.converters.avro.VariantContextToVariantConverter;
import org.opencb.biodata.tools.variant.metadata.VariantMetadataManager;
import org.opencb.hpg.bigdata.core.io.avro.AvroFileWriter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * Created by jtarraga on 03/08/16.
 */
public class VariantAvroSerializer extends AvroSerializer<VariantAvro> {

    private String species = null;
    private String assembly = null;
    private String datasetName = null;

    public VariantAvroSerializer(String species, String assembly, String datasetName,
                                 String compression) {
        super(compression);
        this.species = species;
        this.assembly = assembly;
        this.datasetName = datasetName;
    }

    public void toAvro(String inputFilename, String outputFilename, boolean annotate) throws IOException {
        File inputFile = new File(inputFilename);
        String filename = inputFile.getName();

        VariantMetadataManager metadataManager = new VariantMetadataManager();

        // VCF reader
        VcfFileReader vcfFileReader = new VcfFileReader();
        vcfFileReader.open(inputFilename);
        VCFHeader vcfHeader = vcfFileReader.getVcfHeader();

        // Avro writer
        OutputStream outputStream;
        if (StringUtils.isEmpty(outputFilename) || outputFilename.equals("STDOUT")) {
            outputStream = System.out;
        } else {
            outputStream = new FileOutputStream(outputFilename);
        }
        AvroFileWriter<VariantAvro> avroFileWriter = new AvroFileWriter<>(VariantAvro.SCHEMA$, compression, outputStream);
        avroFileWriter.open();
//        VariantGlobalStatsCalculator statsCalculator = new VariantGlobalStatsCalculator(vcfReader.getSource());
//        statsCalculator.pre();

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
        List<VariantContext> variantContexts = vcfFileReader.read(2000);
        if (annotate) {
            // Annotate before converting to Avro
            // Duplicate code for efficiency purposes
            VariantAvroAnnotator variantAvroAnnotator = new VariantAvroAnnotator();
            List<VariantAvro> variants = new ArrayList<>(2000);

            while (variantContexts.size() > 0) {
                for (VariantContext vc : variantContexts) {
                    Variant variant = converter.convert(vc);
                    if (filter(variant.getImpl())) {
                        counter++;
                        variants.add(variant.getImpl());
//                    statsCalculator.updateGlobalStats(variant);
                    }
                }
                // Annotate variants and then write them to disk
                List<VariantAvro> annotatedVariants = variantAvroAnnotator.annotate(variants);
                for (VariantAvro annotatedVariant: annotatedVariants) {
                    // Write to disk
                    avroFileWriter.writeDatum(annotatedVariant);
                }
                variantContexts = vcfFileReader.read(2000);
            }
        } else {
            // Convert to Avro without annotating
            while (variantContexts.size() > 0) {
                for (VariantContext vc : variantContexts) {
                    Variant variant = converter.convert(vc);
                    if (filter(variant.getImpl())) {
                        counter++;
                        avroFileWriter.writeDatum(variant.getImpl());
//                    statsCalculator.updateGlobalStats(variant);
                    }
                }
                variantContexts = vcfFileReader.read(2000);
            }
        }
        System.out.println("Number of processed records: " + counter);

        // Close
        vcfFileReader.close();
        avroFileWriter.close();
        outputStream.close();

        // Save metadata (JSON format)
        metadataManager.save(Paths.get(outputFilename + ".meta.json"), true);
    }

    public VariantAvroSerializer addRegionFilter(Region region) {
        addFilter(v -> v.getChromosome().equals(region.getChromosome())
                && v.getEnd() >= region.getStart()
                && v.getStart() <= region.getEnd());
        return this;
    }

    public VariantAvroSerializer addRegionFilter(List<Region> regions, boolean and) {
        List<Predicate<VariantAvro>> predicates = new ArrayList<>();
        regions.forEach(r -> predicates.add(v -> v.getChromosome().equals(r.getChromosome())
                && v.getEnd() >= r.getStart()
                && v.getStart() <= r.getEnd()));
        addFilter(predicates, and);
        return this;
    }

    public VariantAvroSerializer addValidIdFilter() {
        addFilter(v -> v.getId() != null && !v.getId().isEmpty() && !v.getId().equals("."));
        return this;
    }
}
