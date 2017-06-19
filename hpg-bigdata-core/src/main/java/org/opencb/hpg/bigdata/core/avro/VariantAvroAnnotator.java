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

package org.opencb.hpg.bigdata.core.avro;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.cellbase.client.config.ClientConfiguration;
import org.opencb.cellbase.client.config.RestConfig;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.cellbase.client.rest.VariationClient;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.commons.utils.FileUtils;

import java.io.*;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by imedina on 09/08/16.
 */
public class VariantAvroAnnotator {

    private CellBaseClient cellBaseClient;

    public VariantAvroAnnotator() {
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setVersion("v4");
        String url = "http://bioinfo.hpc.cam.ac.uk/cellbase/webservices/";
        clientConfiguration.setRest(new RestConfig(Collections.singletonList(url), 30000));
        //clientConfiguration.setRest(new RestConfig(Collections
//                .singletonList("http://bioinfodev.hpc.cam.ac.uk/cellbase-4.5.0-beta"), 30000));
        cellBaseClient = new CellBaseClient("hsapiens", clientConfiguration);
    }

    public void annotate(Path avroPath, Path annotatedAvroPath) throws IOException {
        FileUtils.checkFile(avroPath);
        FileUtils.checkDirectory(annotatedAvroPath.getParent(), true);

        InputStream inputStream = new FileInputStream(avroPath.toFile());
        DatumReader<VariantAvro> datumReader = new SpecificDatumReader<>(VariantAvro.SCHEMA$);
        DataFileStream<VariantAvro> dataFileStream = new DataFileStream<>(inputStream, datumReader);

        OutputStream outputStream = new FileOutputStream(annotatedAvroPath.toFile());
        DatumWriter<VariantAvro> datumWriter = new SpecificDatumWriter<>();
        DataFileWriter<VariantAvro> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(VariantAvro.SCHEMA$, outputStream);
//        dataFileWriter.setCodec(CodecFactory.deflateCodec(CodecFactory.DEFAULT_DEFLATE_LEVEL));

        VariationClient variationClient = cellBaseClient.getVariationClient();

        List<String> variants = new ArrayList<>(2000);
        List<VariantAvro> records = new ArrayList<>(2000);
        VariantAvro record;
        int counter = 1, batchSize = 200;
        while (dataFileStream.hasNext()) {
            record = dataFileStream.next();

            records.add(record);
            variants.add(record.getChromosome() + ":" + record.getStart() + ":" + record.getReference() + ":" + record.getAlternate());

            if (counter++ % batchSize == 0) {
                System.out.println("Annotating " + batchSize + " variants batch...");
                QueryResponse<VariantAnnotation> annotations = variationClient.getAnnotations(variants,
                        new QueryOptions(QueryOptions.EXCLUDE, "expression"));
                for (int i = 0; i < annotations.getResponse().size(); i++) {
                    records.get(i).setAnnotation(annotations.getResponse().get(i).first());
                    dataFileWriter.append(records.get(i));
                }

                dataFileWriter.flush();
                records.clear();
                variants.clear();
            }
        }

        // annotate remaining variants
        if (records.size() > 0) {
            QueryResponse<VariantAnnotation> annotations = variationClient.getAnnotations(variants,
                    new QueryOptions(QueryOptions.EXCLUDE, "expression"));
            for (int i = 0; i < annotations.getResponse().size(); i++) {
                records.get(i).setAnnotation(annotations.getResponse().get(i).first());
                dataFileWriter.append(records.get(i));
            }

            dataFileWriter.flush();
        }

        dataFileWriter.close();

        inputStream.close();
        dataFileStream.close();
    }
}
