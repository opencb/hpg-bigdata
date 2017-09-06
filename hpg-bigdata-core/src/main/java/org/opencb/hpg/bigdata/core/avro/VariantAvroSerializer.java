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

    public void toAvro(String inputFilename, String outputFilename) throws IOException {
//
//        throw new UnsupportedOperationException("Avro conversion not implemented yet!");
//
//        VcfManager vcfManager = new VcfManager(Paths.get(inputFilename));
//        vcfManager.


//             Path path = Paths.get(getClass().getResource(filename).toURI());
//        VcfManager vcfManager = new VcfManager(path);
//        index(vcfManager);
//        VcfIterator<VariantContext> iterator = vcfManager.iterator();
//        int count = 0;
//        while (iterator.hasNext()) {
//            VariantContext vc = iterator.next();
//            System.out.println(vc);
//            count++;
//        }
//        assertEquals(3, count);

        File inputFile = new File(inputFilename);
        String filename = inputFile.getName();

        VariantMetadataManager metadataManager = new VariantMetadataManager();

        // reader
        VcfFileReader vcfFileReader = new VcfFileReader();
        vcfFileReader.open(inputFilename);
        VCFHeader vcfHeader = vcfFileReader.getVcfHeader();

        // writer
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
                    avroFileWriter.writeDatum(variant.getImpl());
//                    statsCalculator.updateGlobalStats(variant);
                }
            }
            variantContexts = vcfFileReader.read(1000);
        }
        System.out.println("Number of processed records: " + counter);

        // close
        vcfFileReader.close();
        avroFileWriter.close();
        outputStream.close();

        // save metadata (JSON format)
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
