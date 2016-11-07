package org.opencb.hpg.bigdata.core.avro;

import htsjdk.samtools.*;
import org.ga4gh.models.ReadAlignment;
import org.opencb.hpg.bigdata.core.converters.SAMRecord2ReadAlignmentConverter;
import org.opencb.hpg.bigdata.core.io.avro.AvroFileWriter;

import java.io.*;

/**
 * Created by jtarraga on 03/08/16.
 */
public class AlignmentAvroSerializer extends AvroSerializer<ReadAlignment> {

    public AlignmentAvroSerializer() {
        this("deflate");
    }

    public AlignmentAvroSerializer(String compression) {
        super(compression);
    }

    @Override
    public void toAvro(String inputFilename, String outputFilename) throws IOException {

        // reader
        SamReader reader = SamReaderFactory.makeDefault().open(new File(inputFilename));

        // writer
        OutputStream outputStream = new FileOutputStream(outputFilename);
        AvroFileWriter<ReadAlignment> avroFileWriter = new AvroFileWriter<>(ReadAlignment.SCHEMA$,
                                                                            compression, outputStream);
        avroFileWriter.open();

        // converter
        SAMRecord2ReadAlignmentConverter converter = new SAMRecord2ReadAlignmentConverter();

        // main loop
        SAMRecordIterator iterator = reader.iterator();
        while (iterator.hasNext()) {
            SAMRecord record = iterator.next();
            ReadAlignment readAlignment = converter.forward(record);
            if (filter(readAlignment)) {
                avroFileWriter.writeDatum(readAlignment);
            }
        }

        // save the SAM header in a separated file
        PrintWriter pwriter = new PrintWriter(new FileWriter(outputFilename + ".header"));
        pwriter.write(reader.getFileHeader().getTextHeader());
        pwriter.close();

        // close
        reader.close();
        avroFileWriter.close();
        outputStream.close();
    }

    @Override
    public void toAvro(InputStream inputStream, String outputFilename) throws IOException {
        toAvro(inputStream.toString(), outputFilename);
    }
}
