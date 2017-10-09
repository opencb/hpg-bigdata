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

import avro.shaded.com.google.common.base.Throwables;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.cellbase.client.config.ClientConfiguration;
import org.opencb.cellbase.client.config.RestConfig;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.cellbase.client.rest.VariantClient;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.commons.utils.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private Logger logger;
    public VariantAvroAnnotator() {
        logger = LoggerFactory.getLogger(VariantAvroAnnotator.class);

        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setVersion("v4");
        clientConfiguration.setRest(new RestConfig(Collections
//                .singletonList("http://bioinfodev.hpc.cam.ac.uk/cellbase-4.5.0-beta"), 30000));
                .singletonList("http://bioinfo.hpc.cam.ac.uk/cellbase"), 30000));
        cellBaseClient = new CellBaseClient("hsapiens", clientConfiguration);
    }

    public void annotate(Path avroPath, Path annotatedAvroPath) throws IOException {
        FileUtils.checkFile(avroPath);
        FileUtils.checkDirectory(annotatedAvroPath.getParent(), true);

        if (avroPath.toFile().getAbsolutePath().equals(annotatedAvroPath.toFile().getAbsolutePath())) {
            throw new IOException("Both files are the same");
        }

        InputStream inputStream = new FileInputStream(avroPath.toFile());
        DatumReader<VariantAvro> datumReader = new SpecificDatumReader<>(VariantAvro.SCHEMA$);
        DataFileStream<VariantAvro> dataFileStream = new DataFileStream<>(inputStream, datumReader);

        OutputStream outputStream = new FileOutputStream(annotatedAvroPath.toFile());
        DatumWriter<VariantAvro> datumWriter = new SpecificDatumWriter<>();
        DataFileWriter<VariantAvro> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(VariantAvro.SCHEMA$, outputStream);
//        dataFileWriter.setCodec(CodecFactory.deflateCodec(CodecFactory.DEFAULT_DEFLATE_LEVEL));

        VariantClient variantClient = cellBaseClient.getVariantClient();

        List<String> variants = new ArrayList<>(2000);
        List<VariantAvro> records = new ArrayList<>(2000);
        VariantAvro record;
        int counter = 1, batchSize = 200;
        while (dataFileStream.hasNext()) {
            record = dataFileStream.next();

            records.add(record);
            variants.add(record.getChromosome() + ":" + record.getStart() + ":" + record.getReference() + ":" + record.getAlternate());

            if (counter++ % batchSize == 0) {
                logger.debug("Annotating {} variants batch...", batchSize);
                QueryResponse<VariantAnnotation> annotations = variantClient.getAnnotationByVariantIds(variants,
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
            QueryResponse<VariantAnnotation> annotations = variantClient.getAnnotationByVariantIds(variants,
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

    public List<VariantAvro> annotate(List<VariantAvro> variants) {
        VariantClient variantClient = cellBaseClient.getVariantClient();

        List<String> ids = new ArrayList<>(variants.size());
        for (VariantAvro variant: variants) {
            ids.add(variant.getChromosome() + ":" + variant.getStart() + ":" + variant.getReference()
                    + ":" + variant.getAlternate());
        }
        logger.debug("Annotating {} variants batch...", variants.size());
        try {
            QueryResponse<VariantAnnotation> annotations = variantClient.getAnnotationByVariantIds(ids,
                    new QueryOptions(QueryOptions.EXCLUDE, "expression"));
//            assert(variants.size() == annotations.getResponse().size());
            for (int i = 0; i < annotations.getResponse().size(); i++) {
                VariantAnnotation annotation = annotations.getResponse().get(i).first();
                // Patch to remove by updating the Evidence avdl model
                if (annotation.getTraitAssociation() != null) {
                    for (EvidenceEntry evidenceEntry : annotation.getTraitAssociation()) {
                        if (evidenceEntry.getSubmissions() == null) {
                            evidenceEntry.setSubmissions(Collections.emptyList());
                        }
                        if (evidenceEntry.getHeritableTraits() == null) {
                            evidenceEntry.setHeritableTraits(Collections.emptyList());
                        } else {
                            for (HeritableTrait heritableTrait: evidenceEntry.getHeritableTraits()) {
                                if (heritableTrait.getInheritanceMode() == null) {
                                    heritableTrait.setInheritanceMode(ModeOfInheritance.unknown);
                                }
                            }
                        }
                        if (evidenceEntry.getGenomicFeatures() == null) {
                            evidenceEntry.setGenomicFeatures(Collections.emptyList());
                        }
                        if (evidenceEntry.getAdditionalProperties() == null) {
                            evidenceEntry.setAdditionalProperties(Collections.emptyList());
                        }
                        if (evidenceEntry.getEthnicity() == null) {
                            evidenceEntry.setEthnicity(EthnicCategory.Z);
                        }
                        if (evidenceEntry.getBibliography() == null) {
                            evidenceEntry.setBibliography(Collections.emptyList());
                        }
                        if (evidenceEntry.getSomaticInformation() != null) {
                            if (evidenceEntry.getSomaticInformation().getSampleSource() == null) {
                                evidenceEntry.getSomaticInformation().setSampleSource("");
                            }
                            if (evidenceEntry.getSomaticInformation().getTumourOrigin() == null) {
                                evidenceEntry.getSomaticInformation().setTumourOrigin("");
                            }
                        }
                    }
                }
                // TODO This data model is obsolete, this code must be removed
                if (annotation.getVariantTraitAssociation() != null) {
                    if (annotation.getVariantTraitAssociation().getCosmic() != null) {
                        for (Cosmic cosmic: annotation.getVariantTraitAssociation().getCosmic()) {
                            if (cosmic.getSiteSubtype() == null) {
                                cosmic.setSiteSubtype("");
                            }
                            if (cosmic.getSampleSource() == null) {
                                cosmic.setSampleSource("");
                            }
                            if (cosmic.getTumourOrigin() == null) {
                                cosmic.setTumourOrigin("");
                            }
                            if (cosmic.getHistologySubtype() == null) {
                                cosmic.setHistologySubtype("");
                            }
                            if (cosmic.getPrimarySite() == null) {
                                cosmic.setPrimarySite("");
                            }
                            if (cosmic.getPrimaryHistology() == null) {
                                cosmic.setPrimaryHistology("");
                            }
                        }
                    }
                }
                // End of patch
                variants.get(i).setAnnotation(annotation);
            }
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        return variants;
    }
}
