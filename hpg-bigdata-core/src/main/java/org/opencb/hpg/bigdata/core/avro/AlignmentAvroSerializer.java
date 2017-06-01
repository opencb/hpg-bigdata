package org.opencb.hpg.bigdata.core.avro;

import htsjdk.samtools.*;
import org.apache.commons.lang3.StringUtils;
import org.ga4gh.models.ReadAlignment;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.tools.alignment.BamManager;
import org.opencb.biodata.tools.alignment.iterators.AlignmentIterator;
import org.opencb.hpg.bigdata.core.converters.SAMRecord2ReadAlignmentConverter;
import org.opencb.hpg.bigdata.core.io.avro.AvroFileWriter;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * Created by jtarraga on 03/08/16.
 */
public class AlignmentAvroSerializer extends AvroSerializer<ReadAlignment> {

    private static String headerSuffix = ".header";
    private boolean binQualities;

    public AlignmentAvroSerializer() {
        this("deflate", true);
    }

    public AlignmentAvroSerializer(String compression, boolean binQualities) {
        super(compression);
        this.binQualities = binQualities;
    }

    @Override
    public void toAvro(String inputFilename, String outputFilename) throws IOException {

        // reader
        SamReader reader = SamReaderFactory.makeDefault().open(new File(inputFilename));

        // writer
        boolean stdout = false;
        OutputStream outputStream;
        if (StringUtils.isEmpty(outputFilename) || outputFilename.toUpperCase().equals("STDOUT")) {
            outputStream = System.out;
            stdout = true;
        } else {
            outputStream = new FileOutputStream(outputFilename);
        }
        AvroFileWriter<ReadAlignment> avroFileWriter = new AvroFileWriter<>(ReadAlignment.SCHEMA$,
                                                                            compression, outputStream);
        avroFileWriter.open();

        // converter
        SAMRecord2ReadAlignmentConverter converter = new SAMRecord2ReadAlignmentConverter(binQualities);

        // main loop
        long counter = 0;
        SAMRecordIterator iterator = reader.iterator();
        while (iterator.hasNext()) {
            SAMRecord record = iterator.next();
            ReadAlignment readAlignment = converter.forward(record);
            if (filter(readAlignment)) {
                avroFileWriter.writeDatum(readAlignment);
                counter++;
            }
        }
        System.out.println("Number of processed records: " + counter);

        // save the SAM header in a separated file only when an output filename is provided
        if (!stdout) {
            PrintWriter pwriter = new PrintWriter(new FileWriter(outputFilename + headerSuffix));
            pwriter.write(reader.getFileHeader().getTextHeader());
            pwriter.close();
        }

        // close
        reader.close();
        avroFileWriter.close();
        outputStream.close();
    }

    public AlignmentAvroSerializer addRegionFilter(Region region) {
        addFilter(a -> a.getAlignment() != null
                && a.getAlignment().getPosition() != null
                && a.getAlignment().getPosition().getReferenceName().equals(region.getChromosome())
                && a.getAlignment().getPosition().getPosition() <= region.getEnd()
                && (a.getAlignment().getPosition().getPosition() + a.getAlignedSequence().length())
                >= region.getStart());
        return this;
    }

    public AlignmentAvroSerializer addRegionFilter(List<Region> regions, boolean and) {
        List<Predicate<ReadAlignment>> predicates = new ArrayList<>();
        regions.forEach(r -> predicates.add(a -> a.getAlignment() != null
                && a.getAlignment().getPosition() != null
                && a.getAlignment().getPosition().getReferenceName().equals(r.getChromosome())
                && a.getAlignment().getPosition().getPosition() <= r.getEnd()
                && (a.getAlignment().getPosition().getPosition() + a.getAlignedSequence().length())
                >= r.getStart()));
        addFilter(predicates, and);
        return this;
    }

    public AlignmentAvroSerializer addMinMapQFilter(int minMapQ) {
        addFilter(a -> a.getAlignment() != null
                && a.getAlignment().getMappingQuality() >= minMapQ);
        return this;
    }

    public static String getHeaderSuffix() {
        return headerSuffix;
    }
}
