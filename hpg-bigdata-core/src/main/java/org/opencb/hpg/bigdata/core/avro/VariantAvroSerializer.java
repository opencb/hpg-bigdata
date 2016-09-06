package org.opencb.hpg.bigdata.core.avro;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.biodata.tools.variant.VariantVcfHtsjdkReader;
import org.opencb.biodata.tools.variant.stats.VariantGlobalStatsCalculator;
import org.opencb.hpg.bigdata.core.io.avro.AvroFileWriter;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * Created by jtarraga on 03/08/16.
 */
public class VariantAvroSerializer extends AvroSerializer<VariantAvro> {

    public VariantAvroSerializer() {
        super("deflate");
    }

    public VariantAvroSerializer(String compression) {
        super(compression);
    }

    public void toAvro(InputStream inputStream, String outputFilename) throws IOException {

        // reader
        String metaFilename = outputFilename + ".meta";
        VariantSource variantSource = new VariantSource(metaFilename, "0", "0", "s");
        VariantVcfHtsjdkReader vcfReader = new VariantVcfHtsjdkReader(inputStream, variantSource, null);
        vcfReader.open();
        vcfReader.pre();

        // writer
        OutputStream outputStream;
        if (StringUtils.isEmpty(outputFilename) || outputFilename.equals("STDOUT")) {
            outputStream = System.out;
        } else {
            outputStream = new FileOutputStream(outputFilename);
        }
        AvroFileWriter<VariantAvro> avroFileWriter = new AvroFileWriter<>(VariantAvro.SCHEMA$, compression, outputStream);
        avroFileWriter.open();
        VariantGlobalStatsCalculator statsCalculator = new VariantGlobalStatsCalculator(vcfReader.getSource());
        statsCalculator.pre();

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
                    avroFileWriter.writeDatum(variant.getImpl());
                    statsCalculator.updateGlobalStats(variant);
                }
            }
        }
        System.out.println("Number of processed records: " + counter);

        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);

        ObjectWriter writer = mapper.writer();
        PrintWriter pwriter = new PrintWriter(new FileWriter(metaFilename + ".json"));
        pwriter.write(writer.withDefaultPrettyPrinter().writeValueAsString(variantSource.getImpl()));
        pwriter.close();


        // close
        vcfReader.post();
        vcfReader.close();
        avroFileWriter.close();
        outputStream.close();
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
