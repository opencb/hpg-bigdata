package org.opencb.hpg.bigdata.app.cli.local;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AlternateCoordinate;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.biodata.models.variant.metadata.VariantSetStats;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.biodata.tools.variant.metadata.VariantMetadataManager;
import org.opencb.biodata.tools.variant.metadata.VariantMetadataUtils;
import org.opencb.biodata.tools.variant.stats.VariantStatsCalculator;
import org.opencb.hpg.bigdata.core.lib.SparkConfCreator;
import org.opencb.hpg.bigdata.core.lib.VariantDataset;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.mutable.WrappedArray;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.*;

public class SparkTest implements Serializable {

    //public String avroDirname = "/home/jtarraga/data150/partitions";
    //public String avroDirname = "/tmp/chr22.head1k.vcf.avro";
    public String avroDirname = "/tmp/example.vcf.avro";
    public VariantDataset vd;
    public SparkSession sparkSession;
    public transient JavaSparkContext sc;

    public Dataset<Row> ds;
    public JavaRDD<Integer> rdd;

    LinkedHashMap<String, Integer> sortSamplesPosition;

    String inParquet = "/tmp/test.vcf.parquet";
    String inParquet2 = "/tmp/test.vcf.parquet2";
    String inParquet3 = "/tmp/test.vcf.parquet3";

//    public class AvgCount implements Serializable {
//        public AvgCount() {
//            total_ = 0;
//            num_ = 0;
//        }
//        public AvgCount(Integer total, Integer num) {
//            total_ = total;
//            num_ = num;
//        }
//        public AvgCount merge(Iterable<Integer> input) {
//            for (Integer elem : input) {
//                num_ += 1;
//                total_ += elem;
//            }
//            return this;
//        }
//        public Integer total_;
//        public Integer num_;
//        public float avg() {
//            return total_ / (float) num_;
//        }
//    }

//    @Test
//    public void test() {
//        //System.out.println("dataset.count = " + ds.count());
//        //ds.show(4);
//
//        FlatMapFunction<Iterator<Row>, AvgCount> mapF = new FlatMapFunction<Iterator<Row>, AvgCount>() {
//            @Override
//            public Iterator<AvgCount> call(Iterator<Row> input) {
//                AvgCount a = new AvgCount(0, 0);
//                while (input.hasNext()) {
//                    a.total_ += 1; //input.next();
//                    a.num_ += 1;
//                }
//                ArrayList<AvgCount> ret = new ArrayList<AvgCount>();
//                ret.add(a);
//                return ret.iterator();
//            }
//        };
//
//        FlatMapFunction<Iterator<Integer>, AvgCount> mapF2 = new FlatMapFunction<Iterator<Integer>, AvgCount>() {
//            @Override
//            public Iterator<AvgCount> call(Iterator<Integer> input) {
//                AvgCount a = new AvgCount(0, 0);
//                while (input.hasNext()) {
//                    a.total_ += input.next();
//                    a.num_ += 1;
//                }
//                ArrayList<AvgCount> ret = new ArrayList<AvgCount>();
//                ret.add(a);
//                return ret.iterator();
//            }
//        };
//
//        Function2<AvgCount, AvgCount, AvgCount> reduceF = new Function2<AvgCount, AvgCount, AvgCount>() {
//            @Override
//            public AvgCount call(AvgCount a, AvgCount b) {
//                a.total_ += b.total_;
//                a.num_ += b.num_;
//                return a;
//            }
//        };
//
//        Iterator<AvgCount> it = ds.toJavaRDD().mapPartitions(mapF).toLocalIterator();
//        while (it.hasNext()) {
//            System.out.println(it.next().avg());
//        }
//        //AvgCount result = rdd.mapPartitions(mapF2).reduce(reduceF);
//        //AvgCount result = ds.toJavaRDD().mapPartitions(mapF).reduce(reduceF);
//        //System.out.println("\n--------------------\n" + result.avg() + "\n--------------------\n");
//    }

    @Test
    public void test2() {
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));

        List<Integer> list = rdd.mapPartitions((Iterator<Integer> iter) -> {
            List<Integer> outIter = new ArrayList<>(1000);
            while (iter.hasNext()) {
                int val = iter.next();
                outIter.add(val + 1000);
            }
            return outIter.iterator();
        }).collect();
        for (int item: list) {
            System.out.println(item);
        }
    }

    //@Test
    public void test3() {
        List<String> list = ds.toJSON().toJavaRDD().mapPartitions((Iterator<String> iter) -> {
            ObjectMapper objMapper = new ObjectMapper();
            ObjectReader objectReader = objMapper.readerFor(Variant.class);

            List<String> outIter = new ArrayList<>(1000);
            while (iter.hasNext()) {
                String str = iter.next();
                Variant variant = objectReader.readValue(str);
                outIter.add(variant.getType().name());
            }
            return outIter.iterator();
        }).collect();
        for (String item: list) {
            System.out.println(item);
        }
    }

    //@Test
    public void test4() {
        List<Variant> list = ds.toJSON().toJavaRDD().mapPartitions((Iterator<String> iter) -> {
            ObjectMapper objMapper = new ObjectMapper();
            ObjectReader objectReader = objMapper.readerFor(Variant.class);

            List<Variant> outIter = new ArrayList<>(1000);
            while (iter.hasNext()) {
                String str = iter.next();
                Variant variant = objectReader.readValue(str);
                variant.setChromosome("pepe" + variant.getChromosome());
                outIter.add(variant);
            }
            return outIter.iterator();
        }).collect();
        for (Variant item: list) {
            System.out.println(item.toJson());
        }
    }

    //@Test
    public void test5() {
        Encoder<Row> encoder = Encoders.kryo(Row.class);
        String outDir = "/tmp/test5/partitions";
        ds.mapPartitions(new MapPartitionsFunction<Row, Row>() {
            @Override
            public Iterator<Row> call(Iterator<Row> input) throws Exception {
                List<Row> output = new ArrayList<>(1000);
                while (input.hasNext()) {
                    output.add(input.next());
                }
                return output.iterator();
            }
        }, encoder).write().format("com.databricks.spark.avro").save(outDir);
        //Dataset<Row> ds1 = sparkSession.read().format("com.databricks.spark.avro").load(outDir);
    }

    //@Test
    public void test6() {
        //Encoder<VariantAvro> encoder = Encoders.kryo(VariantAvro.class);
        Encoder<VariantAvro> encoder = Encoders.bean(VariantAvro.class);
        String inDir = "/tmp/test6/partitions";

        Dataset<Row> ds2 = sparkSession.read().parquet(inParquet);
        System.out.println(ds2.schema().toString());
        //ds2.printSchema();

//        Dataset<VariantAvro> ds1 = sparkSession.read().parquet(inParquet).as(encoder);
//
//        VariantAvro variantAvro = ds1.head();
//        ObjectMapper objMapper = new ObjectMapper();
//        ObjectWriter objectWriter = objMapper.writerFor(VariantAvro.class);
//        try {
//            System.out.println(objectWriter.withDefaultPrettyPrinter().writeValueAsString(variantAvro));
//        } catch (JsonProcessingException e) {
//            e.printStackTrace();
//        }

        //System.out.println("\n\n\nDataset - printSchema");
        //ds1.printSchema();
        //System.out.println("\n\n\nRow - schema");
        //System.out.println(ds1.head().schema());
        //System.out.println("\n\n\nRow - mkString");
        //System.out.println(ds1.head().schema().mkString());

        //String outDir = "/tmp/test6/partitions";
        //Dataset<VariantAvro> ds1 = sparkSession.read().parquet(inParquet).as(encoder);
        //ds1.toJSON().
        //Dataset<VariantAvro> ds1 = sparkSession.read().parquet(inParquet).as(encoder);
        //Dataset<Row> ds1 = sparkSession.read().parquet(inParquet);
        //ds1.toJSON()
        //Row row;
        //row.schema();
        //ds1.write().parquet(inParquet2);
        //ds1.write().json();
        //ds1.show(true);
        //System.out.println("ds count = " + ds1.count());
/*

        ds.mapPartitions(new MapPartitionsFunction<Variant, Variant>() {
            @Override
            public Iterator<Variant> call(Iterator<Variant> input) throws Exception {
                ObjectMapper objMapper = new ObjectMapper();
                ObjectReader objectReader = objMapper.readerFor(Variant.class);

                List<Variant> output = new ArrayList<>(1000);
                while (input.hasNext()) {
                    Variant variant = objectReader.readValue(input.next().toString());
                    output.add(variant);
                }
                return output.iterator();
            }
        }, encoder).write().format("com.databricks.spark.avro").save(outDir);
        //Dataset<Row> ds1 = sparkSession.read().format("com.databricks.spark.avro").load(outDir);
       */
    }

    public void displayRow(StructField field, int index, Row row, String indent) {
        if (row.get(index) == null) {
            System.out.println(indent + field.name() + " IS NULL");
            return;
        }

//        DataTypes.

        switch (field.dataType().typeName()) {
            case "string":
            case "integer":
                System.out.println(indent + field.name() + ":" + row.get(index) + " -- " + field);
                break;
            case "array":
                System.out.println(field.dataType().json());
                List<String> list = row.getList(index);
                //WrappedArray array = (WrappedArray) obj;
                //Row row;
                //row.getList(i);
                if (list.isEmpty()) {
                    System.out.println(indent + field.name() + ": [] -- " + field);
                } else {
                    System.out.println(indent + field.name() + ": ARRAY NOT EMPTY -- " + field);
                }
                break;
            default:
                System.out.println(field.dataType().json());
                System.out.println(" ?? " + field.dataType().typeName() + " -- " + field);
                break;
        }
    }

    //@Test
    public void test7() throws IOException {
        Dataset<Row> ds1 = sparkSession.read().parquet(inParquet);


        ObjectMapper objMapper = new ObjectMapper();
        ObjectReader objectReader = objMapper.readerFor(VariantAvro.class);

        VariantAvro variantAvro = objectReader.readValue(ds1.toJSON().head());
        Row row = RowFactory.create(variantAvro);
        System.out.println("111\n" + ds1.head().toString());
        System.out.println("222\n" + row.toString());
/*
        Row row = ds1.head();
        System.out.println(row.toString());

        scala.collection.Iterator<StructField> it = row.schema().iterator();
        while (it.hasNext()) {
            StructField field = it.next();
            displayRow(field, row.fieldIndex(field.name()), row, "");
            //System.out.println(field.name() + " : " + field.dataType().typeName());
            //System.out.println(field);
        }
        System.out.println(ds1.toJSON().head());


        //sparkSession.sqlContext().ap applySchema(row, row.schema()).toJSON();
/*
        StructType schema = ds1.head().schema();
        JavaRDD<String> jrdd = ds.toJSON().toJavaRDD().mapPartitions((Iterator<String> iter) -> {
            ObjectMapper objMapper = new ObjectMapper();
            ObjectReader objectReader = objMapper.readerFor(Variant.class);

            List<String> outIter = new ArrayList<>(1000);
            while (iter.hasNext()) {
                String str = iter.next();
                Variant variant = objectReader.readValue(str);
                variant.setChromosome("pepe" + variant.getChromosome());
                outIter.add(variant.toJson());
            }
            return outIter.iterator();
        });

        Dataset<Row> dsRow = sparkSession.read().json(jrdd);
        Dataset<Variant> dsVariant = sparkSession.c

       // for (Variant item: list) {
       //     System.out.println(item.toJson());
       // }
*/
    }

    //@Test
    public void test8() {
        Dataset<Row> ds1 = sparkSession.read().parquet(inParquet);
        StructType schema = ds1.head().schema();

        StructType svSchema = DataTypes.createStructType(
                new StructField[] {
                        DataTypes.createStructField("ciStartLeft", DataTypes.IntegerType, true),
                        DataTypes.createStructField("ciStartRight", DataTypes.IntegerType, true),
                        DataTypes.createStructField("ciEndLeft", DataTypes.IntegerType, true),
                        DataTypes.createStructField("ciEndRight", DataTypes.IntegerType, true),
                        DataTypes.createStructField("copyNumber", DataTypes.IntegerType, true),
                        DataTypes.createStructField("leftSvInsSeq", DataTypes.StringType, true),
                        DataTypes.createStructField("rightSvInsSeq", DataTypes.StringType, true),
                        DataTypes.createStructField("type", DataTypes.StringType, true)
                });

        StructType studySchema = DataTypes.createStructType(
                new StructField[] {
                        DataTypes.createStructField("studyId", DataTypes.StringType, true),
                        DataTypes.createStructField(
                                "files",
                                DataTypes.createArrayType(DataTypes.createStructType(
                                        new StructField[] {
                                                DataTypes.createStructField("fileId", DataTypes.StringType, true),
                                                DataTypes.createStructField("call", DataTypes.StringType, true),
                                                DataTypes.createStructField("attributes", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, true), true)
                                        }), true),
                                true),
                        DataTypes.createStructField(
                                "secondaryAlternates",
                                DataTypes.createArrayType(DataTypes.createStructType(
                                        new StructField[] {
                                                DataTypes.createStructField("chromosome", DataTypes.StringType, true),
                                                DataTypes.createStructField("start", DataTypes.IntegerType, true),
                                                DataTypes.createStructField("end", DataTypes.IntegerType, true),
                                                DataTypes.createStructField("reference", DataTypes.StringType, true),
                                                DataTypes.createStructField("alternate", DataTypes.StringType, true),
                                                DataTypes.createStructField("type", DataTypes.StringType, true)
                                        }), true),
                                true),
                        DataTypes.createStructField("format", DataTypes.createArrayType(DataTypes.StringType, true), true),
                        DataTypes.createStructField("samplesData", DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.StringType,true),true),true),
                        DataTypes.createStructField("stats", DataTypes.createMapType(
                                DataTypes.StringType,
                                DataTypes.createStructType(
                                        new StructField[] {
                                                DataTypes.createStructField("refAllele", DataTypes.StringType, true),
                                                DataTypes.createStructField("altAllele", DataTypes.StringType, true),
                                                DataTypes.createStructField("refAlleleCount", DataTypes.IntegerType, true),
                                                DataTypes.createStructField("altAlleleCount", DataTypes.IntegerType, true),
                                                DataTypes.createStructField("genotypesCount", DataTypes.createMapType(DataTypes.StringType,  DataTypes.IntegerType, true), true),
                                                DataTypes.createStructField("genotypesFreq", DataTypes.createMapType(DataTypes.StringType, DataTypes.FloatType, true), true),
                                                DataTypes.createStructField("missingAlleles", DataTypes.IntegerType,true),
                                                DataTypes.createStructField("missingGenotypes", DataTypes.IntegerType,true),
                                                DataTypes.createStructField("refAlleleFreq", DataTypes.FloatType,true),
                                                DataTypes.createStructField("altAlleleFreq", DataTypes.FloatType,true),
                                                DataTypes.createStructField("maf", DataTypes.FloatType,true),
                                                DataTypes.createStructField("mgf", DataTypes.FloatType,true),
                                                DataTypes.createStructField("mafAllele", DataTypes.StringType,true),
                                                DataTypes.createStructField("mgfGenotype", DataTypes.StringType,true),
                                                DataTypes.createStructField("passedFilters", DataTypes.BooleanType,true),
                                                DataTypes.createStructField("mendelianErrors", DataTypes.IntegerType,true),
                                                DataTypes.createStructField("casesPercentDominant", DataTypes.FloatType,true),
                                                DataTypes.createStructField("controlsPercentDominant", DataTypes.FloatType,true),
                                                DataTypes.createStructField("casesPercentRecessive", DataTypes.FloatType,true),
                                                DataTypes.createStructField("controlsPercentRecessive", DataTypes.FloatType,true),
                                                DataTypes.createStructField("quality", DataTypes.FloatType,true),
                                                DataTypes.createStructField("numSamples", DataTypes.IntegerType,true),
                                                DataTypes.createStructField("variantType", DataTypes.StringType,true),
                                                DataTypes.createStructField(
                                                        "hw",
                                                        DataTypes.createStructType(
                                                                new StructField[] {
                                                                        DataTypes.createStructField("chi2", DataTypes.FloatType, true),
                                                                        DataTypes.createStructField("pValue", DataTypes.FloatType, true),
                                                                        DataTypes.createStructField("n", DataTypes.IntegerType, true),
                                                                        DataTypes.createStructField("n_AA_11", DataTypes.IntegerType, true),
                                                                        DataTypes.createStructField("n_Aa_10", DataTypes.IntegerType, true),
                                                                        DataTypes.createStructField("n_aa_00", DataTypes.IntegerType, true),
                                                                        DataTypes.createStructField("e_AA_11", DataTypes.FloatType, true),
                                                                        DataTypes.createStructField("e_Aa_10", DataTypes.FloatType, true),
                                                                        DataTypes.createStructField("e_aa_00", DataTypes.FloatType, true),
                                                                        DataTypes.createStructField("p", DataTypes.FloatType, true),
                                                                        DataTypes.createStructField("q", DataTypes.FloatType, true)
                                                                }),
                                                        true)
                                        }),
                                true),
                                true)});

        StructType annotationSchema = DataTypes.createStructType(
                new StructField[] {
                        DataTypes.createStructField("chromosome", DataTypes.StringType, true),
                        DataTypes.createStructField("start", DataTypes.IntegerType, true),
                        DataTypes.createStructField("end", DataTypes.IntegerType, true),
                        DataTypes.createStructField("reference", DataTypes.StringType, true),
                        DataTypes.createStructField("alternate", DataTypes.StringType, true),
                        DataTypes.createStructField("ancestralAllele", DataTypes.StringType, true),
                        DataTypes.createStructField("id", DataTypes.StringType, true),
                        DataTypes.createStructField(
                                "xrefs",
                                DataTypes.createArrayType(DataTypes.createStructType(
                                        new StructField[] {
                                                DataTypes.createStructField("id", DataTypes.StringType, true),
                                                DataTypes.createStructField("source", DataTypes.StringType, true)}), true),

                                true),
                        DataTypes.createStructField("hgvs", DataTypes.createArrayType(DataTypes.StringType, true), true),
                        DataTypes.createStructField("displayConsequenceType", DataTypes.StringType, true),
                        DataTypes.createStructField(
                                "consequenceTypes",
                                DataTypes.createArrayType(DataTypes.createStructType(
                                        new StructField[] {
                                                DataTypes.createStructField("geneName", DataTypes.StringType, true),
                                                DataTypes.createStructField("ensemblGeneId", DataTypes.StringType, true),
                                                DataTypes.createStructField("ensemblTranscriptId", DataTypes.StringType, true),
                                                DataTypes.createStructField("strand", DataTypes.StringType, true),
                                                DataTypes.createStructField("biotype", DataTypes.StringType, true),
                                                DataTypes.createStructField(
                                                        "exonOverlap",
                                                        DataTypes.createArrayType(DataTypes.createStructType(
                                                                new StructField[] {
                                                                        DataTypes.createStructField("number", DataTypes.StringType, true),
                                                                        DataTypes.createStructField("percentage", DataTypes.FloatType, true)}), true)
                                                        ,true),
                                                DataTypes.createStructField("transcriptAnnotationFlags", DataTypes.createArrayType(DataTypes.StringType, true), true),
                                                DataTypes.createStructField("cdnaPosition", DataTypes.IntegerType, true),
                                                DataTypes.createStructField("cdsPosition", DataTypes.IntegerType, true),
                                                DataTypes.createStructField("codon", DataTypes.StringType, true)}), true),
                                true)});
/*
                                        DataTypes.createStructField(proteinVariantAnnotation,
                                                StructType(
                                                        DataTypes.createStructField(uniprotAccession,StringType,true),
                                                        DataTypes.createStructField(uniprotName,StringType,true),
                                                        DataTypes.createStructField(position,IntegerType,true),
                                                        DataTypes.createStructField(reference,StringType,true),
                                                        DataTypes.createStructField(alternate,StringType,true),
                                                        DataTypes.createStructField(uniprotVariantId,StringType,true),
                                                        DataTypes.createStructField(functionalDescription,StringType,true),
                                                        DataTypes.createStructField(substitutionScores,ArrayType(
                                                                StructType(
                                                                        DataTypes.createStructField(score,DoubleType,true),
                                                                        DataTypes.createStructField(source,StringType,true),
                                                                        DataTypes.createStructField(description,StringType,true))
                                                                ,true)
                                                                ,true),
                                                        DataTypes.createStructField(keywords,ArrayType(StringType,true),true),
                                                        DataTypes.createStructField(features,ArrayType(
                                                                StructType(
                                                                        DataTypes.createStructField(id,StringType,true),
                                                                        DataTypes.createStructField(start,IntegerType,true),
                                                                        DataTypes.createStructField(end,IntegerType,true),
                                                                        DataTypes.createStructField(type,StringType,true),
                                                                        DataTypes.createStructField(description,StringType,true))
                                                                ,true)
                                                                ,true))
                                                ,true),
                                        DataTypes.createStructField(sequenceOntologyTerms,ArrayType(
                                                StructType(
                                                        DataTypes.createStructField(accession,StringType,true),
                                                        DataTypes.createStructField(name,StringType,true))
                                                ,true)
                                                ,true))
                                ,true)
                                ,true),
                        DataTypes.createStructField(populationFrequencies,ArrayType(
                                StructType(
                                        DataTypes.createStructField(study,StringType,true),
                                        DataTypes.createStructField(population,StringType,true),
                                        DataTypes.createStructField(refAllele,StringType,true),
                                        DataTypes.createStructField(altAllele,StringType,true),
                                        DataTypes.createStructField(refAlleleFreq,FloatType,true),
                                        DataTypes.createStructField(altAlleleFreq,FloatType,true),
                                        DataTypes.createStructField(refHomGenotypeFreq,FloatType,true),
                                        DataTypes.createStructField(hetGenotypeFreq,FloatType,true),
                                        DataTypes.createStructField(altHomGenotypeFreq,FloatType,true))
                                ,true)
                                ,true),
                        DataTypes.createStructField(minorAllele,StringType,true),
                        DataTypes.createStructField(minorAlleleFreq,FloatType,true),
                        DataTypes.createStructField(conservation,ArrayType(
                                StructType(
                                        DataTypes.createStructField(score,DoubleType,true),
                                        DataTypes.createStructField(source,StringType,true),
                                        DataTypes.createStructField(description,StringType,true))
                                ,true)
                                ,true),
                        DataTypes.createStructField(geneExpression,ArrayType(
                                StructType(
                                        DataTypes.createStructField(geneName,StringType,true),
                                        DataTypes.createStructField(transcriptId,StringType,true),
                                        DataTypes.createStructField(experimentalFactor,StringType,true),
                                        DataTypes.createStructField(factorValue,StringType,true),
                                        DataTypes.createStructField(experimentId,StringType,true),
                                        DataTypes.createStructField(technologyPlatform,StringType,true),
                                        DataTypes.createStructField(expression,StringType,true),
                                        DataTypes.createStructField(pvalue,FloatType,true))
                                ,true)
                                ,true),
                        DataTypes.createStructField(geneTraitAssociation,ArrayType(
                                StructType(
                                        DataTypes.createStructField(id,StringType,true),
                                        DataTypes.createStructField(name,StringType,true),
                                        DataTypes.createStructField(hpo,StringType,true),
                                        DataTypes.createStructField(score,FloatType,true),
                                        DataTypes.createStructField(numberOfPubmeds,IntegerType,true),
                                        DataTypes.createStructField(associationTypes,ArrayType(StringType,true),true),
                                        DataTypes.createStructField(sources,ArrayType(StringType,true),true),
                                        DataTypes.createStructField(source,StringType,true))
                                ,true)
                                ,true),
                        DataTypes.createStructField(geneDrugInteraction,ArrayType(
                                StructType(
                                        DataTypes.createStructField(geneName,StringType,true),
                                        DataTypes.createStructField(drugName,StringType,true),
                                        DataTypes.createStructField(source,StringType,true),
                                        DataTypes.createStructField(studyType,StringType,true),
                                        DataTypes.createStructField(type,StringType,true)),true)
                                ,true),
                        DataTypes.createStructField(variantTraitAssociation,
                                StructType(
                                        StructField(clinvar,ArrayType(
                                                StructType(
                                                        StructField(accession,StringType,true),
                                                        StructField(clinicalSignificance,StringType,true),
                                                        StructField(traits,ArrayType(StringType,true),true),
                                                        StructField(geneNames,ArrayType(StringType,true),true),
                                                        StructField(reviewStatus,StringType,true))
                                                ,true)
                                                ,true),
                                        StructField(gwas,ArrayType(
                                                StructType(
                                                        StructField(snpIdCurrent,StringType,true),
                                                        StructField(traits,ArrayType(StringType,true),true),
                                                StructField(riskAlleleFrequency,DoubleType,true),
                                                StructField(reportedGenes,StringType,true))
                                        ,true)
                                        ,true),
                                StructField(cosmic,ArrayType(
                                        StructType(
                                                StructField(mutationId,StringType,true),
                                                StructField(primarySite,StringType,true),
                                                StructField(siteSubtype,StringType,true),
                                                StructField(primaryHistology,StringType,true),
                                                StructField(histologySubtype,StringType,true),
                                                StructField(sampleSource,StringType,true),
                                                StructField(tumourOrigin,StringType,true),
                                                StructField(geneName,StringType,true),
                                                StructField(mutationSomaticStatus,StringType,true))
                                        ,true)
                                        ,true))
                        ,true),
                        StructField(traitAssociation,ArrayType(
                                StructType(
                                        StructField(source,
                                                StructType(
                                                        StructField(name,StringType,true),
                                                        StructField(version,StringType,true),
                                                        StructField(date,StringType,true))
                                                ,true),
                                        StructField(submissions,ArrayType(
                                                StructType(
                                                        StructField(submitter,StringType,true),
                                                        StructField(date,StringType,true),
                                                        StructField(id,StringType,true))
                                                ,true)
                                                ,true),
                                        StructField(somaticInformation,
                                                StructType(
                                                        StructField(primarySite,StringType,true),
                                                        StructField(siteSubtype,StringType,true),
                                                        StructField(primaryHistology,StringType,true),
                                                        StructField(histologySubtype,StringType,true),
                                                        StructField(tumourOrigin,StringType,true),
                                                        StructField(sampleSource,StringType,true))
                                                ,true),
                                        StructField(url,StringType,true),
                                        StructField(id,StringType,true),
                                        StructField(assembly,StringType,true),
                                        StructField(alleleOrigin,ArrayType(StringType,true),true),
                                        StructField(heritableTraits,ArrayType(
                                                StructType(
                                                        StructField(trait,StringType,true),
                                                        StructField(inheritanceMode,StringType,true))
                                                ,true)
                                                ,true),
                                        StructField(genomicFeatures,ArrayType(
                                                StructType(
                                                        StructField(featureType,StringType,true),
                                                        StructField(ensemblId,StringType,true),
                                                        StructField(xrefs,MapType(StringType,StringType,true),true))
                                                ,true)
                                                ,true),
                                        StructField(variantClassification,
                                                StructType(
                                                        StructField(clinicalSignificance,StringType,true),
                                                        StructField(drugResponseClassification,StringType,true),
                                                        StructField(traitAssociation,StringType,true),
                                                        StructField(tumorigenesisClassification,StringType,true),
                                                        StructField(functionalEffect,StringType,true))
                                                ,true),
                                        StructField(impact,StringType,true),
                                        StructField(confidence,StringType,true),
                                        StructField(consistencyStatus,StringType,true),
                                        StructField(ethnicity,StringType,true),
                                        StructField(penetrance,StringType,true),
                                        StructField(variableExpressivity,BooleanType,true),
                                        StructField(description,StringType,true),
                                        StructField(additionalProperties,ArrayType(
                                                StructType(
                                                        StructField(id,StringType,true),
                                                        StructField(name,StringType,true),
                                                        StructField(value,StringType,true))
                                                ,true)
                                                ,true),
                                        StructField(bibliography,ArrayType(StringType,true),true))
                                ,true)
                                ,true),
                        StructField(functionalScore,ArrayType(
                                StructType(
                                        StructField(score,DoubleType,true),
                                        StructField(source,StringType,true),
                                        StructField(description,StringType,true))
                                ,true)
                                ,true),
                        StructField(cytoband,ArrayType(
                                StructType(
                                        StructField(stain,StringType,true),
                                        StructField(name,StringType,true),
                                        StructField(start,IntegerType,true),
                                        StructField(end,IntegerType,true))
                                ,true)
                                ,true),
                        StructField(repeat,ArrayType(
                                StructType(
                                        StructField(id,StringType,true),
                                        StructField(chromosome,StringType,true),
                                        StructField(start,IntegerType,true),
                                        StructField(end,IntegerType,true),
                                        StructField(period,IntegerType,true),
                                        StructField(copyNumber,FloatType,true),
                                        StructField(percentageMatch,FloatType,true),
                                        StructField(score,FloatType,true),
                                        StructField(sequence,StringType,true),
                                        StructField(source,StringType,true))
                                ,true)
                                ,true),
                        StructField(additionalAttributes,MapType(StringType,
                                StructType(
                                        StructField(attribute,MapType(StringType,StringType,true),true))
                                ,true)
                                ,true))

        }
        );
*/
        StructType schema2 = DataTypes.createStructType(
                new StructField[] {
                        DataTypes.createStructField("id", DataTypes.StringType, true),
                        DataTypes.createStructField("names", DataTypes.createArrayType(DataTypes.StringType, true), true),
                        DataTypes.createStructField("chromosome", DataTypes.StringType, true),
                        DataTypes.createStructField("start", DataTypes.IntegerType, true),
                        DataTypes.createStructField("end", DataTypes.IntegerType, true),
                        DataTypes.createStructField("reference", DataTypes.StringType, true),
                        DataTypes.createStructField("alternate", DataTypes.StringType, true),
                        DataTypes.createStructField("strand", DataTypes.StringType, true),
                        DataTypes.createStructField("sv", svSchema, true),
                        DataTypes.createStructField("length", DataTypes.IntegerType, true),
                        DataTypes.createStructField("type", DataTypes.StringType, true),
                        DataTypes.createStructField("hgvs", DataTypes.createMapType(DataTypes.StringType, DataTypes.createArrayType(DataTypes.StringType, true), true), true),
                        DataTypes.createStructField("studies", DataTypes.createArrayType(studySchema, true), true),
                        DataTypes.createStructField("annotation", annotationSchema, true)
                }
        );

//        Iterator<StructField> iterator = (Iterator<StructField>) ds1.head().schema().iterator();
//        while (iterator.hasNext()) {
//            System.out.println(iterator.next());
//        }

        Dataset<Row> dsRow = ds1.mapPartitions(new MapPartitionsFunction<Row, Row>() {
            @Override
            public Iterator<Row> call(Iterator<Row> input) throws Exception {
                List<Row> output = new ArrayList<>(1000);
                while (input.hasNext()) {
                    VariantAvro variantAvro = new VariantAvro();
                    Row inputRow = input.next();
//                    Row outputRow = RowFactory.create(
//                            inputRow.getSeq(0),    // id
//                            inputRow.getList(1),   // names
//                            inputRow.getString(2), // chromosome
//                            inputRow.getInt(3),    // start
//                            inputRow.getInt(4),    // end
//                            inputRow.getString(5), // reference
//                            inputRow.getString(6), // alternate
//                            inputRow.getString(7), // strand
//                            inputRow.get(8),       // sv
//                            inputRow.getInt(9),    // length
//                            inputRow.getString(10),// type
//                            inputRow.getMap(11),   // hgvs
//                            inputRow.get(12),      // studies
//                            inputRow.get(13)       // annotation
//                    );
                    //output.add(outputRow);
                    output.add(inputRow);
                }
                return output.iterator();
            }
        }, RowEncoder.apply(schema));
        System.out.println("row count = " + dsRow.count());

        // gzip, snappy, lzo, uncompressed
        dsRow.write().parquet(inParquet2);

        Dataset<Row> ds2 = sparkSession.read().parquet(inParquet2);
        dsRow.write().parquet(inParquet3);

    }

    public Row computeVariantStats(Row inputRow) {
        Variant variant = null;
        for (StudyEntry entry : variant.getStudies()) {
            VariantStats stats = null; //new VariantStats(inputRow.getString(5), inputRow.getString(6), VariantType.INDEL); // VariantType(inputRow.getString(10)));
            VariantStatsCalculator.calculate(entry, entry.getAttributes(), null, stats);
            entry.setStats(StudyEntry.DEFAULT_COHORT, stats);
        }

        VariantStats stats = new VariantStats();
        stats.setAltAllele("A");
        stats.setAltAlleleFreq(0.0004f);

        Row outputRow = RowFactory.create(
                stats.getRefAllele(),
                stats.getAltAllele(),
                stats.getRefAlleleCount(),
                stats.getAltAlleleCount(),
                null, //JavaConverters.asScalanull,
                null,
                stats.getMissingAlleles(),
                stats.getMissingGenotypes(),
                stats.getRefAlleleFreq(),
                stats.getAltAlleleFreq(),
                stats.getMaf(),
                stats.getMgf(),
                stats.getMafAllele(),
                stats.getMgfGenotype(),
                stats.getPassedFilters(),
                stats.getMendelianErrors(),
                stats.getCasesPercentDominant(),
                stats.getControlsPercentDominant(),
                stats.getCasesPercentRecessive(),
                stats.getControlsPercentRecessive(),
                stats.getQuality(),
                stats.getNumSamples(),
                stats.getVariantType().toString(),
                (stats.getHw() == null ? null : RowFactory.create(
                        stats.getHw().getChi2(),
                        stats.getHw().getPValue(),
                        stats.getHw().getN(),
                        stats.getHw().getNAA11(),
                        stats.getHw().getNAa10(),
                        stats.getHw().getNAa00(),
                        stats.getHw().getEAa00(),
                        stats.getHw().getEAa10(),
                        stats.getHw().getEAa00(),
                        stats.getHw().getP(),
                        stats.getHw().getQ())
                )
        );

        return outputRow;
    }

    public StudyEntry convertRow2StudyEntry(Row studyRow, LinkedHashMap<String, Integer> samplePosition) {
        StudyEntry studyEntry = new StudyEntry();

        // study ID
        studyEntry.setStudyId(studyRow.getString(0));
//        System.out.println("study ID = " + studyEntry.getStudyId());

        // files
        List<Row> fileRows = studyRow.getList(1);
        List<FileEntry> files = new ArrayList<>();
        for (Row fileRow: fileRows) {
            FileEntry fileEntry = new FileEntry();
            fileEntry.setFileId(fileRow.getString(0));
            fileEntry.setCall(fileRow.getString(1));
            fileEntry.setAttributes(fileRow.getJavaMap(2));
            files.add(fileEntry);
        }
//        studyEntry.setFiles(files);
//        for (FileEntry file: studyEntry.getFiles()) {
//            System.out.println("file = " + file.toString());
//        }

        // secondaryAlternates
        List<AlternateCoordinate> secondaryAlternates = new ArrayList<>();
        List<Row> secondaryAlternatesRows = studyRow.getList(2);
        for (Row secondaryAlternatesRow: secondaryAlternatesRows) {
//            secondaryAlternates.add(new AlternateCoordinate(
//                    secondaryAlternatesRow.getString(0),
//                    secondaryAlternatesRow.getInt(1),
//                    secondaryAlternatesRow.getInt(2),
//                    secondaryAlternatesRow.getString(3),
//                    secondaryAlternatesRow.getString(4),
//                    VariantType.INDEL));
        }
//        studyEntry.setSecondaryAlternates(secondaryAlternates);
//        for (AlternateCoordinate alt: studyEntry.getSecondaryAlternates()) {
//            System.out.println("secondary alterante = " + alt.toString());
//        }

        // format
        List<String> format = studyRow.getList(3);
        studyEntry.setFormat(format);
//        for (String f: studyEntry.getFormat()) {
//            System.out.println("format = " + f);
//        }

        // samplesData
        List<List<String>> samplesData = new ArrayList<>();
        List<Object> list = studyRow.getList(4);
        for (Object item: list) {
            List<String> l1 = new ArrayList<String>();
            scala.collection.Iterator it = ((WrappedArray) item).iterator();
            while (it.hasNext()) {
                l1.add((String) it.next());
            }
            samplesData.add(l1);
        }
        studyEntry.setSamplesData(samplesData);
//        for (List<String> l: studyEntry.getSamplesData()) {
//            System.out.println("--");
//            for (String str: l) {
//                System.out.println("\tsample data = " + str);
//            }
//        }

        // set sorted samples position map
        studyEntry.setSamplesPosition(samplePosition);

        System.out.println(studyEntry.toString());
        return studyEntry;
    }

    public Row convertVariantStats2Row(VariantStats variantStats) {
        Row row = RowFactory.create(
                variantStats.getRefAllele(),
                variantStats.getAltAllele(),
                variantStats.getRefAlleleCount(),
                variantStats.getAltAlleleCount(),
                null, //JavaConverters.asScalanull,
                null,
                variantStats.getMissingAlleles(),
                variantStats.getMissingGenotypes(),
                variantStats.getRefAlleleFreq(),
                variantStats.getAltAlleleFreq(),
                variantStats.getMaf(),
                variantStats.getMgf(),
                variantStats.getMafAllele(),
                variantStats.getMgfGenotype(),
                variantStats.getPassedFilters(),
                variantStats.getMendelianErrors(),
                variantStats.getCasesPercentDominant(),
                variantStats.getControlsPercentDominant(),
                variantStats.getCasesPercentRecessive(),
                variantStats.getControlsPercentRecessive(),
                variantStats.getQuality(),
                variantStats.getNumSamples(),
                variantStats.getVariantType().toString(),
                (variantStats.getHw() == null ? null : RowFactory.create(
                        variantStats.getHw().getChi2(),
                        variantStats.getHw().getPValue(),
                        variantStats.getHw().getN(),
                        variantStats.getHw().getNAA11(),
                        variantStats.getHw().getNAa10(),
                        variantStats.getHw().getNAa00(),
                        variantStats.getHw().getEAa00(),
                        variantStats.getHw().getEAa10(),
                        variantStats.getHw().getEAa00(),
                        variantStats.getHw().getP(),
                        variantStats.getHw().getQ())
                )
        );
        return row;
    }

    public Row updateRowStats(Row inputRow, LinkedHashMap<String, Integer> sortSamplesPosition) {
        List<Row> inputRowStudies = inputRow.getList(12);
        Row outputRowStudies[] = new Row[inputRowStudies.size()];

        for (int i = 0; i < inputRowStudies.size(); i++) {
            Row studyRow = inputRowStudies.get(i);

            // convert: Row -> StudyEntry
            StudyEntry studyEntry = convertRow2StudyEntry(studyRow, sortSamplesPosition);

            // compute stats for cohort ALL
            //VariantStats variantStats = new VariantStats(inputRow.getString(5), inputRow.getString(6), VariantType.INDEL);
            //VariantStatsCalculator.calculate(studyEntry, studyEntry.getAllAttributes(), null, variantStats);

            // convert: VariantStats -> Row
            //Row statsRow = convertVariantStats2Row(variantStats);

            // create stats map
            Map<String, Row> stats = new HashMap<>();
            // stats.put("ALL", statsRow);

            // create row for that study with the updated stats
            Row outputRowStudy = RowFactory.create(
                    studyRow.get(0), // studyId
                    studyRow.get(1), // files
                    studyRow.get(2), // secondaryAlternates
                    studyRow.get(3), // format
                    studyRow.get(4), // samplesData
                    JavaConverters.mapAsScalaMapConverter(stats).asScala().toMap(Predef.conforms()));

            outputRowStudies[i] = outputRowStudy;
        }

        Row outputRow = RowFactory.create(
                inputRow.get(0),  // id
                inputRow.get(1),  // names
                inputRow.get(2),  // chromosome
                inputRow.get(3),  // start
                inputRow.get(4),  // end
                inputRow.get(5),  // reference
                inputRow.get(6),  // alternate
                inputRow.get(7),  // strand
                inputRow.get(8),  // sv
                inputRow.get(9),  // length
                inputRow.get(10), // type
                inputRow.get(11), // hgvs
                outputRowStudies, // studies: inputRow.get(12)
                inputRow.get(13)  // annotation
        );

        return outputRow;
    }

    //@Test
    public void test9() {
        Dataset<Row> ds1 = sparkSession.read().parquet(inParquet);
        Row inputRow = ds1.head();

        Row outputRow = updateRowStats(inputRow, sortSamplesPosition);
        System.out.println(inputRow.toString());
        System.out.println(outputRow.toString());
    }

    //@Test
    public void test10() {
        Dataset<Row> ds1 = sparkSession.read().parquet(inParquet);
        Row row1 = ds1.head();
        StructType schema = row1.schema();

        ds1.mapPartitions(new MapPartitionsFunction<Row, Row>() {
            @Override
            public Iterator<Row> call(Iterator<Row> input) throws Exception {
                List<Row> output = new ArrayList<>(1000);
                while (input.hasNext()) {
                    output.add(updateRowStats(input.next(), sortSamplesPosition));
                }
                return output.iterator();
            }
        }, RowEncoder.apply(schema)).write().parquet(inParquet2);

        Dataset<Row> ds2 = sparkSession.read().parquet(inParquet2);
        Row row2 = ds2.head();

        System.out.println(row1.toString());
        System.out.println(row2.toString());
    }

    //@Test
    public void test11() {
        VariantMetadataManager manager = new VariantMetadataManager();
        try {
            manager.load(Paths.get(inParquet + ".meta.json"));
            List<String> samples = VariantMetadataUtils.getSampleNames(manager.getVariantMetadata().getStudies().get(0));
            sortSamplesPosition = new LinkedHashMap<>();
            for (int i = 0; i < samples.size(); i++) {
                sortSamplesPosition.put(samples.get(i), i);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        Dataset<Row> ds1 = sparkSession.read().parquet(inParquet);
        Row row1 = ds1.head();

        StructType schema = row1.schema();

        Broadcast<LinkedHashMap> broad = sc.broadcast(sortSamplesPosition);
        ds1.map(new MapFunction<Row, Row>() {
            @Override
            public Row call(Row input) throws Exception {
                return updateRowStats(input, broad.getValue());
            }
        }, RowEncoder.apply(schema)).write().parquet(inParquet2);

        Dataset<Row> ds2 = sparkSession.read().parquet(inParquet2);
        Row row2 = ds2.head();

        ds2.write().parquet(inParquet3);

        System.out.println(row1.toString());
        System.out.println(row2.toString());
    }

    //@Test
    public void test12() {
        Dataset<Row> ds1 = sparkSession.read().parquet(inParquet);
        Row row1 = ds1.head();
        StructType schema = row1.schema();

        ds1.map(new MapFunction<Row, Row>() {
            @Override
            public Row call(Row input) throws Exception {
                return updateRowStats(input, sortSamplesPosition);
            }
        }, Encoders.bean(Row.class)).write().parquet(inParquet2);

        Dataset<Row> ds2 = sparkSession.read().parquet(inParquet2);
        Row row2 = ds2.head();

        ds2.write().parquet(inParquet3);

        System.out.println(row1.toString());
        System.out.println(row2.toString());
    }

    //@Test
    public void test13() {
        Dataset<Row> ds1 = sparkSession.read().parquet(inParquet);
        Row row1 = ds1.head();

        for (Object row: row1.getList(12)) {
            convertRow2StudyEntry((Row) row, sortSamplesPosition);
        }
    }

    //@Test
    public void test14() {
        VariantMetadataManager manager = new VariantMetadataManager();
        try {
            manager.load(Paths.get(inParquet + ".meta.json"));
            List<String> samples = VariantMetadataUtils.getSampleNames(manager.getVariantMetadata().getStudies().get(0));
            sortSamplesPosition = new LinkedHashMap<>();
            for (int i = 0; i < samples.size(); i++) {
                sortSamplesPosition.put(samples.get(i), i);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        Dataset<Row> ds1 = sparkSession.read().parquet(inParquet);
        Row row1 = ds1.head();

        StructType schema = row1.schema();


        //VariantSetStatsCalculator calculator = new VariantSetStatsCalculator();

        Broadcast<LinkedHashMap> broad = sc.broadcast(sortSamplesPosition);
//        ds1.map(new MapFunction<org.opencb.hpg.bigdata.app.cli.local.avro.VariantAvro, Map<String, VariantSetStats>>() {
//            @Override
//            public Map<String, VariantSetStats> call(Row input) throws Exception {
//                return updateRowStats(input, broad.getValue());
//            }
//        }, RowEncoder.apply(schema)).rewrite().parquet(inParquet2);

        Dataset<Row> ds2 = sparkSession.read().parquet(inParquet2);
        Row row2 = ds2.head();

        ds2.write().parquet(inParquet3);

        System.out.println(row1.toString());
        System.out.println(row2.toString());
    }

    //@Test
    public void test15() {
        VariantConvertCLITest variantConvertCLITest = new VariantConvertCLITest();
        variantConvertCLITest.vcf2parquet();
        vd = new VariantDataset(sparkSession);
        try {
            vd.load(variantConvertCLITest.parquetPath.toString());
            vd.createOrReplaceTempView("vcf");

            vd.show(10, false);

            // Generate the schema based on the string of schema
            final int numValues = 1092;
            List<StructField> fields = new ArrayList<>();
            for (int i = 0; i < numValues; i++) {
                StructField field = DataTypes.createStructField("v" + i, DataTypes.IntegerType, true);
                //StructField field = DataTypes.createStructField("v", DataTypes.createArrayType(DataTypes.DoubleType), true);
                fields.add(field);
            }
            StructType schema = DataTypes.createStructType(fields);

            JavaRDD<Vector> rows = vd.getDs().toJavaRDD().map((Function<Row, Vector>) r -> {
                // Studies, by default get first study
                List<Row> studies = r.getList(12);
                Row study = studies.get(0);

                // samplesData
                List<List<String>> samplesData = new ArrayList<>();
                List<Object> list = study.getList(4);
                int i = 0;
                double values[] = new double[numValues];
                for (Object item: list) {
                    String gt = (String) ((WrappedArray) item).head();
                    if (gt.equals("0|0")) {
                        values[i] = 0;
                    } else if (gt.equals("1|1")) {
                        values[i] = 2;
                    } else {
                        values[i] = 1;
                    }
                    i++;
                }
                Vector output = Vectors.dense(values);
                return output;
            });

            //System.out.println("first = " + rows.first());
            //Dataset<Vector> gts = sparkSession.createDataFrame(jrdd, schema);
            //gts.show(10, false);


            Iterator<Vector> iterator = rows.collect().iterator();
            while (iterator.hasNext()) {
                Vector vector = iterator.next();
                System.out.println(vector.size() + ": " + iterator.next());
            }

//
//            System.out.println("row = " + jrdd.first());
//
//
//            Dataset<Row> peopleDataFrame = sparkSession.createDataFrame(jrdd, schema);
//            peopleDataFrame.show(1);

//            // $example on$
//            List<Vector> data = new ArrayList<>();
//            data.add(Vectors.sparse(5, new int[] {1, 3}, new double[] {1.0, 7.0}));
//            data.add(Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0));
//            data.add(Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0));
//
//            JavaRDD<Vector> rows = sc.parallelize(data);
//
//            System.out.println(rows.take(1).toString());
//
            // Create a RowMatrix from JavaRDD<Vector>.
            RowMatrix mat = new RowMatrix(rows.rdd());

//            // Compute the top 4 principal components.
//            // Principal components are stored in a local dense matrix.
            Matrix pc = mat.computePrincipalComponents(4);
//
//            // Project the rows to the linear space spanned by the top 4 principal components.
//            RowMatrix projected = mat.multiply(pc);
//            // $example off$
//            Vector[] collectPartitions = (Vector[])projected.rows().collect();
//            System.out.println("Projected vector of principal component:");
//            for (Vector vector : collectPartitions) {
//                System.out.println("\t" + vector);
//            }

//            System.out.println(vd.first());
//
//
//            RowEncoder.
            //vd.getDs().col()
            //StructType schema2 = vd.schema();

            //Dataset<Row> ds = vd.getDs();

            //StructType schema1 = ds.schema();
            //StructType schema = DataTypes.createStructType(new StructField[] { DataTypes.createStructField("id", DataTypes.DoubleType, false)});
            //StructType newSchema = DataTypes.createStructType(Collections.singleton(DataTypes.createArrayType(DataTypes.DoubleType, true)));
            //Row row = RowFactory.create(3.0, 45.6, 5.6);
            //System.out.println(row.schema().toString());
//            Dataset<Row> doubleDs = ds.map(new MapFunction<Row, Row>() {
//                @Override
//                public Row call(Row input) throws Exception {
//                    Row output = RowFactory.create(Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0));
//                    return output;
//                    //return input;
//                }
//            }, RowEncoder.apply(schema));
//            //System.out.println("count = " + doubleDs.count());
//            doubleDs.show();


//            ds.map(new Function1<Row, Row>() {
//                @Override
//                public Row apply(Row row) {
//                    RowFactory.create(Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0));
//                    Row outputRow = null;
//                    return outputRow;
//                }
//            }, RowEncoder.class);
//
//            //JavaRDD<Vector> rows = jsc.parallelize(data);
//
//            List<Vector> data = Arrays.asList(
//                    Vectors.sparse(5, new int[] {1, 3}, new double[] {1.0, 7.0}),
//                    Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
//                    Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
//            );
//            Vector vector1 = new Vector(Arrays.asList("1,2,3,4,5".split(",")));
//            Vectors.dense()
//
//
//            // Create a RowMatrix from JavaRDD<Vector>.
//            RowMatrix mat = new RowMatrix(rows.rdd());
//
//            // Compute the top 4 principal components.
//            // Principal components are stored in a local dense matrix.
//            Matrix pc = mat.computePrincipalComponents(4);
//
//            // Project the rows to the linear space spanned by the top 4 principal components.
//            RowMatrix projected = mat.multiply(pc);
//            // $example off$
//            Vector[] collectPartitions = (Vector[])projected.rows().collect();
//            System.out.println("Projected vector of principal component:");
//            for (Vector vector : collectPartitions) {
//                System.out.println("\t" + vector);
//            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Before
    public void setUp() throws Exception {
        VariantRvTestsCLITest rvTestsCLITest = new VariantRvTestsCLITest();
        //rvTestsCLITest.init();

        SparkConf sparkConf = SparkConfCreator.getConf("TEST", "local", 1, true);
        sparkConf.set("spark.driver.allowMultipleContexts", "true");

        sc = new JavaSparkContext(sparkConf);
        sparkSession = new SparkSession(sc.sc());
        sparkSession.sqlContext().setConf("spark.sql.parquet.compression.codec", "snappy");
        //sparkSession.sqlContext().setConf("spark.sql.parquet.compression.codec", "gzip");
        //SparkSession sparkSession = new SparkSession(new SparkContext(sparkConf));

        // ds = sparkSession.sqlContext().read().format("com.databricks.spark.avro").load(avroDirname);

        rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));

        //ds = sparkSession.sql("SELECT * FROM vcf");
/*
        vd = new VariantDataset(sparkSession);
        read();
*/
/*
        try {
            String filename = "/tmp/chr22.head1k.vcf.avro";
            //String filename = "/tmp/chr22.head100k.vcf.avro";
            //String filename = rvTestsCLITest.avroPath.toString();

            vd.load(filename);
        } catch (Exception e) {
            e.printStackTrace();
        }
        vd.createOrReplaceTempView("vcf");
*/
        sortSamplesPosition = new LinkedHashMap<>();
        sortSamplesPosition.put("1", 0);
        sortSamplesPosition.put("2", 1);
        sortSamplesPosition.put("3", 2);
        sortSamplesPosition.put("4", 3);
        sortSamplesPosition.put("5", 4);
        sortSamplesPosition.put("6", 5);

    }

    @After
    public void tearDown() {
        sparkSession.stop();
    }

    //@Test
    public void map() {
        ObjectMapper objMapper = new ObjectMapper();
        ObjectReader objectReader = objMapper.readerFor(Variant.class);

//        Broadcast<ObjectReader> broad = sc.broadcast(objectReader);

        List<Integer> list = vd.toJavaRDD().map((Function<Row, Integer>) s -> {
            //System.out.println(s);
            return 1;
        }).collect();

        System.out.println("list size = " + list.size());
    }



    //@Test
    public void mapToPair() {
        ObjectMapper objMapper = new ObjectMapper();
        ObjectReader objectReader = objMapper.readerFor(Variant.class);

        Broadcast<ObjectReader> broad = sc.broadcast(objectReader);

        List<Tuple2<String, Integer>> list = vd.toJSON().toJavaRDD().mapToPair((PairFunction<String, String, Integer>) s -> {
            Variant variant = broad.getValue().readValue(s);
            String key = variant.getChromosome();
            int value = variant.getStart();
            return new Tuple2<>(key, value);
        }).collect();
        for (Tuple2<String, Integer> item: list) {
            System.out.println(item._1 + " -> " + item._2);
        }
    }

    //@Test
    public void reduce() {
        ObjectMapper objMapper = new ObjectMapper();
        ObjectReader objectReader = objMapper.readerFor(Variant.class);

        Broadcast<ObjectReader> broad = sc.broadcast(objectReader);

        List<Tuple2<String, Integer>> list = vd.toJSON().toJavaRDD().mapToPair((PairFunction<String, String, Integer>) s -> {
            Variant variant = broad.getValue().readValue(s);
            String key = "" + variant.getStart(); //variant.getChromosome();
            int value = variant.getStart();
            return new Tuple2<>(key, value);
        }).reduceByKey((Function2<Integer, Integer, Integer>) (i1, i2) -> {
            return i1 + i2;
        }).collect();
        for (Tuple2<String, Integer> item: list) {
            System.out.println(item._1 + " -> " + item._2);
        }
    }

    //@Test
    public void reduceStats() {
        ObjectMapper objMapper = new ObjectMapper();
        ObjectReader objectReader = objMapper.readerFor(Variant.class);

        Broadcast<ObjectReader> broad = sc.broadcast(objectReader);

        List<Tuple2<String, VariantSetStats>> list = vd.toJSON().toJavaRDD().mapToPair((PairFunction<String,
                String, VariantSetStats>) s -> {
            Variant variant = broad.getValue().readValue(s);
            String key = "" + variant.getStart(); //variant.getChromosome();
            VariantSetStats value = new VariantSetStats();
            value.setNumVariants(1);
            return new Tuple2<>(key, value);
        }).reduceByKey((Function2<VariantSetStats, VariantSetStats, VariantSetStats>) (stats1, stats2) -> {
            stats1.setNumVariants(stats1.getNumVariants() + stats2.getNumVariants());
            return stats1;
        }).collect();
        for (Tuple2<String, VariantSetStats> item: list) {
            System.out.println(item._1 + " -> " + item._2().getNumVariants());
        }
    }

    //@Test
    public void write() {
        String tmpDir = "/home/jtarraga/data150/partitions";
        vd.repartition(10).write().format("com.databricks.spark.avro").save(tmpDir);
        //vd.coalesce(10).write().format("com.databricks.spark.avro").save(tmpDir);
        //vd.write().format("com.databricks.spark.avro").save(tmpDir);
    }

    //@Test
    public void read() {
        String tmpDir = "/home/jtarraga/data150/partitions";
        try {
            vd.load(tmpDir);
            vd.createOrReplaceTempView("vcf");
            partition();
            System.out.println("number of rows = " + vd.count());
        } catch (Exception e) {
            e.printStackTrace();
        }
        //vd.coalesce(10).write().format("com.databricks.spark.avro").save(tmpDir);
        //vd.write().format("com.databricks.spark.avro").save(tmpDir);
    }

    //@Test
    public void partition() {
        List<Partition> list = vd.toJSON().toJavaRDD().partitions();
        for (Partition item: list) {
            System.out.println(item.toString());
        }
    }

    //@Test
    public void mapPartition() {
        List<Partition> list = vd.toJSON().toJavaRDD().mapPartitions(
                new FlatMapFunction<Iterator<String>, VariantSetStats>() {
                    @Override
                    public Iterator<VariantSetStats> call(Iterator<String> stringIterator) throws Exception {
                        return null;
                    }
                }).partitions(); //.mapPartitions()partitions();
        for (Partition item: list) {
            System.out.println(item.toString());
        }
    }


    //@Test
    public void reduceStatsMap() {
        ObjectMapper objMapper = new ObjectMapper();
        ObjectReader objectReader = objMapper.readerFor(Variant.class);

        Broadcast<ObjectReader> broad = sc.broadcast(objectReader);

        List<Tuple2<String, Map<String, VariantSetStats>>> list = vd.toJSON().toJavaRDD().mapToPair((PairFunction<String,
                String, Map<String, VariantSetStats>>) s -> {
            Variant variant = broad.getValue().readValue(s);
            String key = "" + variant.getStart(); //variant.getChromosome();
            Map<String, VariantSetStats> value = new HashMap();
            VariantSetStats stats = new VariantSetStats();
            stats.setNumVariants(1);
            value.put("" + variant.getStart(), stats);
            return new Tuple2<>(key, value);
        }).reduceByKey((Function2<Map<String, VariantSetStats>,
                Map<String, VariantSetStats>, Map<String, VariantSetStats>>) (stats1, stats2) -> {
            for (String key: stats2.keySet()) {
                if (stats1.containsKey(key)) {
                    stats1.get(key).setNumVariants(stats1.get(key).getNumVariants() + stats2.get(key).getNumVariants());
                } else {
                    stats1.put(key, stats2.get(key));
                }
            }
            return stats1;
        }).collect();
        for (Tuple2<String, Map<String, VariantSetStats>> item: list) {
            System.out.println(item._1 + " -> ");
            Map<String, VariantSetStats> map = item._2();
            for (String key: map.keySet()) {
                System.out.println("\t" + key + ", num. variants = " + map.get(key).getNumVariants());
            }
        }
    }

}



/*

        StructType(
                StructField(id,StringType,true),
                StructField(names,ArrayType(StringType,true),true),
                StructField(chromosome,StringType,true),
                StructField(start,IntegerType,true),
                StructField(end,IntegerType,true),
                StructField(reference,StringType,true),
                StructField(alternate,StringType,true),
                StructField(strand,StringType,true),
                StructField(sv,
                        StructType(
                                StructField(ciStartLeft,IntegerType,true),
                                StructField(ciStartRight,IntegerType,true),
                                StructField(ciEndLeft,IntegerType,true),
                                StructField(ciEndRight,IntegerType,true),
                                StructField(copyNumber,IntegerType,true),
                                StructField(leftSvInsSeq,StringType,true),
                                StructField(rightSvInsSeq,StringType,true),
                                StructField(type,StringType,true)
                        ),true),
                StructField(length,IntegerType,true),
                StructField(type,StringType,true),
                StructField(hgvs,MapType(StringType,ArrayType(StringType,true),true),true),
                StructField(studies,ArrayType(
                        StructType(
                                StructField(studyId,StringType,true),
                                StructField(files,ArrayType(
                                        StructType(
                                                StructField(fileId,StringType,true),
                                                StructField(call,StringType,true),
                                                StructField(attributes,MapType(StringType,StringType,true),true))
                                        ,true)
                                        ,true),
                                StructField(secondaryAlternates,ArrayType(
                                        StructType(
                                                StructField(chromosome,StringType,true),
                                                StructField(start,IntegerType,true),
                                                StructField(end,IntegerType,true),
                                                StructField(reference,StringType,true),
                                                StructField(alternate,StringType,true),
                                                StructField(type,StringType,true))
                                        ,true)
                                        ,true),
                                StructField(format,ArrayType(StringType,true),true),
                                StructField(samplesData,ArrayType(ArrayType(StringType,true),true),true),
                                StructField(stats,MapType(StringType,
                                        StructType(
                                                StructField(refAllele,StringType,true),
                                                StructField(altAllele,StringType,true),
                                                StructField(refAlleleCount,IntegerType,true),
                                                StructField(altAlleleCount,IntegerType,true),
                                                StructField(genotypesCount,MapType(StringType,IntegerType,true),true),
                                                StructField(genotypesFreq,MapType(StringType,FloatType,true),true),
                                                StructField(missingAlleles,IntegerType,true),
                                                StructField(missingGenotypes,IntegerType,true),
                                                StructField(refAlleleFreq,FloatType,true),
                                                StructField(altAlleleFreq,FloatType,true),
                                                StructField(maf,FloatType,true),
                                                StructField(mgf,FloatType,true),
                                                StructField(mafAllele,StringType,true),
                                                StructField(mgfGenotype,StringType,true),
                                                StructField(passedFilters,BooleanType,true),
                                                StructField(mendelianErrors,IntegerType,true),
                                                StructField(casesPercentDominant,FloatType,true),
                                                StructField(controlsPercentDominant,FloatType,true),
                                                StructField(casesPercentRecessive,FloatType,true),
                                                StructField(controlsPercentRecessive,FloatType,true),
                                                StructField(quality,FloatType,true),
                                                StructField(numSamples,IntegerType,true),
                                                StructField(variantType,StringType,true),
                                                StructField(hw,
                                                        StructType(
                                                                StructField(chi2,FloatType,true),
                                                                StructField(pValue,FloatType,true),
                                                                StructField(n,IntegerType,true),
                                                                StructField(n_AA_11,IntegerType,true),
                                                                StructField(n_Aa_10,IntegerType,true),
                                                                StructField(n_aa_00,IntegerType,true),
                                                                StructField(e_AA_11,FloatType,true),
                                                                StructField(e_Aa_10,FloatType,true),
                                                                StructField(e_aa_00,FloatType,true),
                                                                StructField(p,FloatType,true),
                                                                StructField(q,FloatType,true))
                                                        ,true))
                                        ,true)
                                        ,true))
                        ,true)
                        ,true),
                StructField(annotation,
                        StructType(
                                StructField(chromosome,StringType,true),
                                StructField(start,IntegerType,true),
                                StructField(end,IntegerType,true),
                                StructField(reference,StringType,true),
                                StructField(alternate,StringType,true),
                                StructField(ancestralAllele,StringType,true),
                                StructField(id,StringType,true),
                                StructField(xrefs,ArrayType(
                                        StructType(
                                                StructField(id,StringType,true),
                                                StructField(source,StringType,true))
                                        ,true)
                                        ,true),
                                StructField(hgvs,ArrayType(StringType,true),true),
                                StructField(displayConsequenceType,StringType,true),
                                StructField(consequenceTypes,ArrayType(
                                        StructType(
                                                StructField(geneName,StringType,true),
                                                StructField(ensemblGeneId,StringType,true),
                                                StructField(ensemblTranscriptId,StringType,true),
                                                StructField(strand,StringType,true),
                                                StructField(biotype,StringType,true),
                                                StructField(exonOverlap,ArrayType(
                                                        StructType(
                                                                StructField(number,StringType,true),
                                                                StructField(percentage,FloatType,true))
                                                        ,true)
                                                        ,true),
                                                StructField(transcriptAnnotationFlags,ArrayType(StringType,true),true),
                                                StructField(cdnaPosition,IntegerType,true),
                                                StructField(cdsPosition,IntegerType,true),
                                                StructField(codon,StringType,true),
                                                StructField(proteinVariantAnnotation,
                                                        StructType(
                                                                StructField(uniprotAccession,StringType,true),
                                                                StructField(uniprotName,StringType,true),
                                                                StructField(position,IntegerType,true),
                                                                StructField(reference,StringType,true),
                                                                StructField(alternate,StringType,true),
                                                                StructField(uniprotVariantId,StringType,true),
                                                                StructField(functionalDescription,StringType,true),
                                                                StructField(substitutionScores,ArrayType(
                                                                        StructType(
                                                                                StructField(score,DoubleType,true),
                                                                                StructField(source,StringType,true),
                                                                                StructField(description,StringType,true))
                                                                        ,true)
                                                                        ,true),
                                                                StructField(keywords,ArrayType(StringType,true),true),
                                                                StructField(features,ArrayType(
                                                                        StructType(
                                                                                StructField(id,StringType,true),
                                                                                StructField(start,IntegerType,true),
                                                                                StructField(end,IntegerType,true),
                                                                                StructField(type,StringType,true),
                                                                                StructField(description,StringType,true))
                                                                        ,true)
                                                                        ,true))
                                                        ,true),
                                                StructField(sequenceOntologyTerms,ArrayType(
                                                        StructType(
                                                                StructField(accession,StringType,true),
                                                                StructField(name,StringType,true))
                                                        ,true)
                                                        ,true))
                                        ,true)
                                        ,true),
                                StructField(populationFrequencies,ArrayType(
                                        StructType(
                                                StructField(study,StringType,true),
                                                StructField(population,StringType,true),
                                                StructField(refAllele,StringType,true),
                                                StructField(altAllele,StringType,true),
                                                StructField(refAlleleFreq,FloatType,true),
                                                StructField(altAlleleFreq,FloatType,true),
                                                StructField(refHomGenotypeFreq,FloatType,true),
                                                StructField(hetGenotypeFreq,FloatType,true),
                                                StructField(altHomGenotypeFreq,FloatType,true))
                                        ,true)
                                        ,true),
                                StructField(minorAllele,StringType,true),
                                StructField(minorAlleleFreq,FloatType,true),
                                StructField(conservation,ArrayType(
                                        StructType(
                                                StructField(score,DoubleType,true),
                                                StructField(source,StringType,true),
                                                StructField(description,StringType,true))
                                        ,true)
                                        ,true),
                                StructField(geneExpression,ArrayType(
                                        StructType(
                                                StructField(geneName,StringType,true),
                                                StructField(transcriptId,StringType,true),
                                                StructField(experimentalFactor,StringType,true),
                                                StructField(factorValue,StringType,true),
                                                StructField(experimentId,StringType,true),
                                                StructField(technologyPlatform,StringType,true),
                                                StructField(expression,StringType,true),
                                                StructField(pvalue,FloatType,true))
                                        ,true)
                                        ,true),
                                StructField(geneTraitAssociation,ArrayType(
                                        StructType(
                                                StructField(id,StringType,true),
                                                StructField(name,StringType,true),
                                                StructField(hpo,StringType,true),
                                                StructField(score,FloatType,true),
                                                StructField(numberOfPubmeds,IntegerType,true),
                                                StructField(associationTypes,ArrayType(StringType,true),true),
                                                StructField(sources,ArrayType(StringType,true),true),
                                                StructField(source,StringType,true))
                                        ,true)
                                        ,true),
                                StructField(geneDrugInteraction,ArrayType(
                                        StructType(
                                                StructField(geneName,StringType,true),
                                                StructField(drugName,StringType,true),
                                                StructField(source,StringType,true),
                                                StructField(studyType,StringType,true),
                                                StructField(type,StringType,true)),true)
                                        ,true), StructField(variantTraitAssociation,
                                        StructType(StructField(clinvar,ArrayType(
                                                StructType(
                                                        StructField(accession,StringType,true),
                                                        StructField(clinicalSignificance,StringType,true),
                                                        StructField(traits,ArrayType(StringType,true),true),
                                                        StructField(geneNames,ArrayType(StringType,true),true),
                                                        StructField(reviewStatus,StringType,true))
                                                ,true)
                                                ,true),
                                                StructField(gwas,ArrayType(
                                                        StructType(
                                                                StructField(snpIdCurrent,StringType,true),
                                                                StructField(traits,ArrayType(StringType,true),true),
                                                                StructField(riskAlleleFrequency,DoubleType,true),
                                                                StructField(reportedGenes,StringType,true))
                                                        ,true)
                                                        ,true),
                                                StructField(cosmic,ArrayType(
                                                        StructType(
                                                                StructField(mutationId,StringType,true),
                                                                StructField(primarySite,StringType,true),
                                                                StructField(siteSubtype,StringType,true),
                                                                StructField(primaryHistology,StringType,true),
                                                                StructField(histologySubtype,StringType,true),
                                                                StructField(sampleSource,StringType,true),
                                                                StructField(tumourOrigin,StringType,true),
                                                                StructField(geneName,StringType,true),
                                                                StructField(mutationSomaticStatus,StringType,true))
                                                        ,true)
                                                        ,true))
                                        ,true),
                                StructField(traitAssociation,ArrayType(
                                        StructType(
                                                StructField(source,
                                                        StructType(
                                                                StructField(name,StringType,true),
                                                                StructField(version,StringType,true),
                                                                StructField(date,StringType,true))
                                                        ,true),
                                                StructField(submissions,ArrayType(
                                                        StructType(
                                                                StructField(submitter,StringType,true),
                                                                StructField(date,StringType,true),
                                                                StructField(id,StringType,true))
                                                        ,true)
                                                        ,true),
                                                StructField(somaticInformation,
                                                        StructType(
                                                                StructField(primarySite,StringType,true),
                                                                StructField(siteSubtype,StringType,true),
                                                                StructField(primaryHistology,StringType,true),
                                                                StructField(histologySubtype,StringType,true),
                                                                StructField(tumourOrigin,StringType,true),
                                                                StructField(sampleSource,StringType,true))
                                                        ,true),
                                                StructField(url,StringType,true),
                                                StructField(id,StringType,true),
                                                StructField(assembly,StringType,true),
                                                StructField(alleleOrigin,ArrayType(StringType,true),true),
                                                StructField(heritableTraits,ArrayType(
                                                        StructType(
                                                                StructField(trait,StringType,true),
                                                                StructField(inheritanceMode,StringType,true))
                                                        ,true)
                                                        ,true),
                                                StructField(genomicFeatures,ArrayType(
                                                        StructType(
                                                                StructField(featureType,StringType,true),
                                                                StructField(ensemblId,StringType,true),
                                                                StructField(xrefs,MapType(StringType,StringType,true),true))
                                                        ,true)
                                                        ,true),
                                                StructField(variantClassification,
                                                        StructType(
                                                                StructField(clinicalSignificance,StringType,true),
                                                                StructField(drugResponseClassification,StringType,true),
                                                                StructField(traitAssociation,StringType,true),
                                                                StructField(tumorigenesisClassification,StringType,true),
                                                                StructField(functionalEffect,StringType,true))
                                                        ,true),
                                                StructField(impact,StringType,true),
                                                StructField(confidence,StringType,true),
                                                StructField(consistencyStatus,StringType,true),
                                                StructField(ethnicity,StringType,true),
                                                StructField(penetrance,StringType,true),
                                                StructField(variableExpressivity,BooleanType,true),
                                                StructField(description,StringType,true),
                                                StructField(additionalProperties,ArrayType(
                                                        StructType(
                                                                StructField(id,StringType,true),
                                                                StructField(name,StringType,true),
                                                                StructField(value,StringType,true))
                                                        ,true)
                                                        ,true),
                                                StructField(bibliography,ArrayType(StringType,true),true))
                                        ,true)
                                        ,true),
                                StructField(functionalScore,ArrayType(
                                        StructType(
                                                StructField(score,DoubleType,true),
                                                StructField(source,StringType,true),
                                                StructField(description,StringType,true))
                                        ,true)
                                        ,true),
                                StructField(cytoband,ArrayType(
                                        StructType(
                                                StructField(stain,StringType,true),
                                                StructField(name,StringType,true),
                                                StructField(start,IntegerType,true),
                                                StructField(end,IntegerType,true))
                                        ,true)
                                        ,true),
                                StructField(repeat,ArrayType(
                                        StructType(
                                                StructField(id,StringType,true),
                                                StructField(chromosome,StringType,true),
                                                StructField(start,IntegerType,true),
                                                StructField(end,IntegerType,true),
                                                StructField(period,IntegerType,true),
                                                StructField(copyNumber,FloatType,true),
                                                StructField(percentageMatch,FloatType,true),
                                                StructField(score,FloatType,true),
                                                StructField(sequence,StringType,true),
                                                StructField(source,StringType,true))
                                        ,true)
                                        ,true),
                                StructField(additionalAttributes,MapType(StringType,
                                        StructType(
                                                StructField(attribute,MapType(StringType,StringType,true),true))
                                        ,true)
                                        ,true))
                        ,true))


 */