package org.opencb.hpg.bigdata.core.avro;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.biodata.models.variant.avro.VariantFileMetadata;
import org.opencb.biodata.tools.variant.VariantVcfHtsjdkReader;
import org.opencb.biodata.tools.variant.stats.VariantGlobalStatsCalculator;
import org.opencb.hpg.bigdata.core.io.avro.AvroFileWriter;

import java.io.*;
import java.util.List;

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
        OutputStream outputStream = new FileOutputStream(outputFilename);
        AvroFileWriter<VariantAvro> avroFileWriter = new AvroFileWriter<>(VariantAvro.SCHEMA$,
                                                                          compression, outputStream);
        avroFileWriter.open();
        VariantGlobalStatsCalculator statsCalculator = new VariantGlobalStatsCalculator(vcfReader.getSource());
        statsCalculator.pre();

        // main loop
        List<Variant> variants;
        while (true) {
            variants = vcfReader.read(1000);
            if (variants.size() == 0) {
                break;
            }
            // write variants and update stats
            for (Variant variant: variants) {
                if (filter(variant.getImpl())) {
                    avroFileWriter.writeDatum(variant.getImpl());
                    statsCalculator.updateGlobalStats(variant);
                }
            }
        }

        // write variant metadata (header, stats,...) in a separated file
        FileOutputStream out = new FileOutputStream(metaFilename);
        AvroFileWriter<VariantFileMetadata> aw = new AvroFileWriter<>(VariantFileMetadata.SCHEMA$,
                                                                      compression, out);
        aw.open();
        aw.writeDatum(variantSource.getImpl());
        aw.close();
        out.close();

        // close
        vcfReader.post();
        vcfReader.close();
        avroFileWriter.close();
        outputStream.close();
    }
}
