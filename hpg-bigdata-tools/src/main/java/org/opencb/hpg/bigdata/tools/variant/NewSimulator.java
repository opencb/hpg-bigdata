package org.opencb.hpg.bigdata.tools.variant;

import org.apache.avro.mapred.AvroKey;
//import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAvro;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

/**
 * Created by kalyanreddyemani on 06/10/15.
 */
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

public class NewSimulator {
    private static Random rand = new Random();
    public static class SimulatorAvroMapper extends
            Mapper<LongWritable, Text, AvroKey<VariantAvro>, NullWritable> {
        private String chromosome = null;
        private int start = 0;
        private int end = 0;
        private String ID = null;
        private String referenceAllele=null;
        private String alternateAllele=null;

        private String variantType = null;
        private String strand = null;
        private List<StudyEntry> variantSourceEntry = new ArrayList<>();
        @Override
        protected void setup(
                Context context)
                throws IOException, InterruptedException {
            chromosome = genChromose(0 , 23);
            start = start();
            end = end();
            if (end < start) {
                end = start + rand.nextInt(9);
            }
            ID = genID();
            referenceAllele = getReference("BACD");
            alternateAllele = getAlternate("BACD");
            variantType = getVariantType();
            strand = genID();
            variantSourceEntry = getStudies(100);
            super.setup(context);
        }
        @Override
        public void map(LongWritable key, Text value, Context context) throws
                IOException, InterruptedException {
            Variant variant=new Variant(chromosome, start, end, referenceAllele , alternateAllele , strand);
            variant.setStudies(variantSourceEntry);
            VariantAvro variantAvro=variant.getImpl();
            context.write(new AvroKey<>(variantAvro), NullWritable.get());
        }
    }
    public int run(String [] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Simulator");
        job.setJarByClass(NewSimulator.class);
        AvroJob.setMapOutputKeySchema(job, VariantAvro.getClassSchema());
        AvroJob.setMapOutputValueSchema(job, VariantAvro.getClassSchema());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setNumReduceTasks(0);
        job.setInputFormatClass(NLinesInputFormat.class);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);
        job.setMapperClass(SimulatorAvroMapper.class);
        return (job.waitForCompletion(true) ? 0 : 1);
    }
    public static String genChromose(int min, int max) {
        int randomNum = rand.nextInt((max - min) + 1) + min;
        return Integer.toString(randomNum);
    }
    public static int start() {
        int n = 100000 + rand.nextInt(900000);
        return n;
    }
    public static int end() {
        int n = 100000 + rand.nextInt(900000);
        return n;
    }
    public static String genID() {
        Random rand = new Random();
        int n = 100000 + rand.nextInt(900000);
        return "rs" + Integer.toString(n);
    }

    public static String getReference(String alleles) {
        final int n = alleles.length();
        Character s=alleles.charAt((rand.nextInt(n)));
        return s.toString();

    }
    public static String getAlternate(String alleles) {
        final int n = alleles.length();
        Character s=alleles.charAt((rand.nextInt(n)));
        return s.toString();

    }
    public static String getVariantType() {
        List<String> variants = new LinkedList<String>();
        variants.add("SNP");
        variants.add("MNP");
        variants.add("SNV");
        variants.add("INDEL");
        variants.add("SV");
        variants.add("CNV");
        variants.add("NO_VARIATION");
        variants.add("SYMBOLIC");
        variants.add("MIXED");
        int randomNumber = rand.nextInt(variants.size());
        return variants.get(randomNumber);
    }

    public static List<StudyEntry> getStudies(int n) {
        int studyID = rand.nextInt(1000 - 0 + 1000) + 0;
        int fieldID = rand.nextInt(1000 - 0 + 1000) + 0;
        Variant variant=new Variant();
        List<StudyEntry> variantSourceEntryList=new ArrayList<>();
        StudyEntry variantSourceEntry = new StudyEntry();
        variantSourceEntry.setStudyId(Integer.toString(studyID));
        variantSourceEntry.setFileId(Integer.toString(fieldID));
        variantSourceEntry.setFormat(getFormat());
        List<List<String>> sampleList = new ArrayList<>(getFormat().size());
        for (int i=0; i < n; i++) {
            sampleList.add(getRandomample());
        }
        variantSourceEntry.setSamplesData(sampleList);
        variantSourceEntryList.add(variantSourceEntry);
        return variantSourceEntryList;
    }
    private static List<String> getRandomample() {
        List<String> sample=new ArrayList<>();
        int gtValue1 = rand.nextInt(1 - 0 + 1) + 0;
        int gtValue2 = rand.nextInt(1 - 0 + 1) + 0;
        int gqValue = rand.nextInt(100 - 0 + 100) + 0;
        int dpValue = rand.nextInt(100 - 0 + 100) + 0;
        int hqValue = rand.nextInt(100 - 0 + 100) + 0;

        sample.add(gtValue1 + "/" + gtValue2);
        sample.add(Integer.toString(gqValue));
        sample.add(Integer.toString(dpValue));
        sample.add(Integer.toString(hqValue));
        return sample;
    }
    public static List<String> getFormat() {

        List<String> formatFields = new ArrayList<>(10);

        formatFields.add("GT");
        formatFields.add("GQ");
        formatFields.add("DP");
        formatFields.add("HQ");
        return formatFields;
    }
}
