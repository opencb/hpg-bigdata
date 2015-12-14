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
package org.opencb.hpg.bigdata.tools.variant;

/**
 * Created by pawan on 19/11/15.
 */
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.io.AvroDataHack;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.StringWriter;
import java.util.*;

/**
 * A class to customize Hadoop partitioning, sorting and grouping for MapReduce jobs that
 * use {@link AvroKey} map output keys.
 */
public class AvroSort {

    static final JsonFactory FACTORY = new JsonFactory();
    static final ObjectMapper MAPPER = new ObjectMapper(FACTORY);

    public static final String PARTITIONING_FIELD_POSITIONS = "avro.sort.partitioningfilepos";
    public static final String SORTING_FIELD_POSITIONS = "avro.sort.sortingfilepos";
    public static final String GROUPING_FIELD_POSITIONS = "avro.sort.groupingfilepos";

    static {
        FACTORY.enable(JsonParser.Feature.ALLOW_COMMENTS);
        FACTORY.setCodec(MAPPER);
    }

    public static String schemaFieldsToJson(Map<Schema, List<AvroDataHack.OrderedField>> schemaFields) {
        try {
            StringWriter writer = new StringWriter();
            JsonGenerator gen = FACTORY.createJsonGenerator(writer);

            gen.writeStartObject();
            gen.writeFieldName("schemas");
            gen.writeStartArray();
            for (Map.Entry<Schema, List<AvroDataHack.OrderedField>> entry: schemaFields.entrySet()) {
                gen.writeStartObject();
                gen.writeStringField("schema", entry.getKey().toString());
                gen.writeFieldName("fields");
                gen.writeStartArray();
                for (AvroDataHack.OrderedField field: entry.getValue()) {
                    gen.writeStartObject();
                    gen.writeNumberField("pos", field.getField().pos());
                    gen.writeBooleanField("asc", field.isAscendingOrder());
                    gen.writeEndObject();
                }
                gen.writeEndArray();
                gen.writeEndObject();
            }
            gen.writeEndArray();
            gen.writeEndObject();
            gen.flush();
            //System.out.println("Created JSON " + writer.toString());
            return writer.toString();
        } catch (IOException e) {
            throw new AvroRuntimeException(e);
        }
    }

    public static Map<Schema, List<AvroDataHack.OrderedField>> jsonToSchemaFields(String json) {
        Map<Schema, List<AvroDataHack.OrderedField>> schemas = new HashMap<>();
        try {
            JsonParser parser = FACTORY.createJsonParser(json);

            JsonNode root = MAPPER.readTree(parser);

            JsonNode node = root.get("schemas");

            for (JsonNode schemaNode : node) {
                List<AvroDataHack.OrderedField> fields = new ArrayList<>();

                Schema schema = new Schema.Parser().parse(schemaNode.get("schema").getValueAsText());

                //System.out.println("Got schema: " + schema + " json: " + schemaNode.get("schema").getValueAsText());


                JsonNode fieldsNodes = schemaNode.get("fields");

                for (JsonNode fieldNode : fieldsNodes) {
                    AvroDataHack.OrderedField orderedField = new AvroDataHack.OrderedField();
                    orderedField.setField(schema.getFields().get(fieldNode.get("pos").getIntValue()));
                    orderedField.setAscendingOrder(fieldNode.get("asc").getBooleanValue());
                    fields.add(orderedField);
                }

                schemas.put(schema, fields);
            }
        } catch (IOException e) {
            throw new AvroRuntimeException(e);
        }
        return schemas;
    }

    public static class ConfigFieldFetcher implements AvroDataHack.FieldFetcher {
        private final Map<Schema, List<AvroDataHack.OrderedField>> schemaFields;
        private static final List<AvroDataHack.OrderedField> EMPTY_FIELDS = new ArrayList<>();

        public ConfigFieldFetcher(Configuration conf, String configKey) {
            schemaFields = jsonToSchemaFields(conf.get(configKey));
        }

        @Override
        public List<AvroDataHack.OrderedField> getFields(Schema schema) {
            List<AvroDataHack.OrderedField> fields =  schemaFields.get(schema);
            if (fields != null) {
                return fields;
            }
            return EMPTY_FIELDS;
        }

        @Override
        public String toString() {
            return ToStringBuilder.reflectionToString(this);
        }
    }

    public static class AvroSecondarySortPartitioner extends
            Partitioner<AvroKey<SpecificRecordBase>, AvroValue<SpecificRecordBase>>
            implements Configurable {

        private Configuration conf;
        private ConfigFieldFetcher fetcher;

        @Override
        public Configuration getConf() {
            return conf;
        }

        @Override
        public void setConf(Configuration conf) {
            this.conf = conf;
            fetcher = new ConfigFieldFetcher(conf, PARTITIONING_FIELD_POSITIONS);
        }

        @Override
        public int getPartition(AvroKey<SpecificRecordBase> key, AvroValue<SpecificRecordBase> value, int numPartitions) {
            int hash = AvroDataHack.hashCode(key.datum(), key.datum().getSchema(), fetcher);
            return Math.abs(hash) % numPartitions;
        }
    }

    public static class AvroSortingComparator extends AvroKeyCustomComparator {
        @Override
        public String getConfigName() {
            return SORTING_FIELD_POSITIONS;
        }
    }

    public static class AvroGroupingComparator extends AvroKeyCustomComparator {
        @Override
        public String getConfigName() {
            return GROUPING_FIELD_POSITIONS;
        }
    }


    public abstract static class AvroKeyCustomComparator extends Configured implements RawComparator<AvroKey<SpecificRecordBase>> {
        /** The schema of the Avro data in the key to compare. */
        private Schema mSchema;

        private ConfigFieldFetcher fetcher;
//        private AvroKeyDeserializer deserializer;

        public abstract String getConfigName();

        /** {@inheritDoc} */
        @Override
        public void setConf(Configuration conf) {
            super.setConf(conf);
            if (null != conf) {
                // The MapReduce framework will be using this comparator to sort AvroKey objects
                // output from the map phase, so use the schema defined for the map output key.
                mSchema = AvroJob.getMapOutputKeySchema(conf);

                fetcher = new ConfigFieldFetcher(conf, getConfigName());
                //System.out.println(getConfigName() + " fields: " + fetcher);

//                deserializer = new AvroKeyDeserializer(mSchema, mSchema, conf.getClassLoader());
            }
        }

        /** {@inheritDoc} */
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            /*try {
                AvroKey<SpecificRecordBase> x = deserialize(b1, s1, l1);
                AvroKey<SpecificRecordBase> y = deserialize(b2, s2, l2);
                System.out.println("Comparing " + x + " with " + y);
            } catch (IOException e) {
                e.printStackTrace();
            } */
            int c = AvroDataHack.compare(b1, s1, l1, b2, s2, l2, mSchema, fetcher);
            return c;
        }
         /*
        private AvroKey<SpecificRecordBase> deserialize(final byte[] bytes, int start, int length) throws IOException {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            os.write(bytes, start, length);
            deserializer.open(new ByteArrayInputStream(os.toByteArray()));
            return (AvroKey<SpecificRecordBase>) deserializer.deserialize(null);
        }  */

        /** {@inheritDoc} */
        @Override
        public int compare(AvroKey<SpecificRecordBase> x, AvroKey<SpecificRecordBase> y) {
            return AvroDataHack.compare(x.datum(), y.datum(), mSchema, fetcher);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Job job;
        private Map<Schema, List<AvroDataHack.OrderedField>> orderedPartitioningFields =
                new HashMap<>();
        private Map<Schema, List<AvroDataHack.OrderedField>> orderedSortingFields = new HashMap<>();
        private Map<Schema, List<AvroDataHack.OrderedField>> orderedGroupingFields = new HashMap<>();

        public Builder setJob(Job job) {
            this.job = job;
            return this;
        }

        public void addField(Map<Schema, List<AvroDataHack.OrderedField>> schemaFields, Schema schema,
                             String fieldName, boolean ascending) {
            List<AvroDataHack.OrderedField> fields = schemaFields.get(schema);
            if (fields == null) {
                fields = new ArrayList<>();
                schemaFields.put(schema, fields);
            }
            fields.add(new AvroDataHack.OrderedField().setField(schema.getField(fieldName)).setAscendingOrder(ascending));
        }

        public Builder addPartitionField(Schema schema, String fieldName, boolean ascending) {
            addField(orderedPartitioningFields, schema, fieldName, ascending);
            return this;
        }

        public Builder addSortField(Schema schema, String fieldName, boolean ascending) {
            addField(orderedSortingFields, schema, fieldName, ascending);
            return this;
        }

        public Builder addGroupField(Schema schema, String fieldName, boolean ascending) {
            addField(orderedGroupingFields, schema, fieldName, ascending);
            return this;
        }

        public void configure() {
            job.getConfiguration().set(PARTITIONING_FIELD_POSITIONS, schemaFieldsToJson(orderedPartitioningFields));
            job.getConfiguration().set(SORTING_FIELD_POSITIONS, schemaFieldsToJson(orderedSortingFields));
            job.getConfiguration().set(GROUPING_FIELD_POSITIONS, schemaFieldsToJson(orderedGroupingFields));

            job.setPartitionerClass(AvroSecondarySortPartitioner.class);
            job.setSortComparatorClass(AvroSortingComparator.class);
            job.setGroupingComparatorClass(AvroGroupingComparator.class);

        }
    }

}
