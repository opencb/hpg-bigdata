package org.opencb.hpg.bigdata.analysis.variant;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.mllib.linalg.Matrices;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.mllib.stat.test.ChiSqTestResult;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import org.opencb.biodata.models.core.pedigree.Pedigree;
import org.opencb.biodata.tools.variant.metadata.VariantMetadataManager;
import org.opencb.hpg.bigdata.core.lib.SparkConfCreator;
import org.opencb.hpg.bigdata.core.lib.VariantDataset;
import scala.Serializable;
import scala.Tuple2;
import scala.collection.mutable.StringBuilder;
import scala.collection.mutable.WrappedArray;

import java.io.File;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.desc;

/**
 * Created by jtarraga on 23/12/16.
 */
public class MLTest implements Serializable {

//    SparkConf sparkConf;
//    SparkSession sparkSession;
//    VariantDataset vd;

//    private void init() throws Exception {
//        // it doesn't matter what we set to spark's home directory
//        sparkConf = SparkConfCreator.getConf("AlignmentDatasetTest", "local", 1, true, "");
//        System.out.println("sparkConf = " + sparkConf.toDebugString());
//        sparkSession = new SparkSession(new SparkContext(sparkConf));
//
//    }


//    @Test
    public void streaming() throws Exception {

        // it doesn't matter what we set to spark's home directory
        SparkConf sparkConf = SparkConfCreator.getConf("AlignmentDatasetTest", "local", 1, true, "");
        System.out.println("sparkConf = " + sparkConf.toDebugString());

        SparkSession sparkSession = new SparkSession(new SparkContext(sparkConf));
        VariantDataset vd = new VariantDataset(sparkSession);
//        Path inputPath = Paths.get("/tmp/test.vcf.avro");
//        Path inputPath = Paths.get("/media/data100/jtarraga/data/spark/100.variants.avro");
//        Path inputPath = Paths.get(getClass().getResource("/100.variants.avro").toURI());
        Path inputPath = Paths.get("/home/jtarraga/appl/hpg-bigdata/hpg-bigdata-core/src/test/resources/100.variants.avro");
        System.out.println(">>>> opening file " + inputPath);
        vd.load(inputPath.toString());
        vd.createOrReplaceTempView("vcf");


        Dataset<Row> rows = vd.sqlContext().sql("SELECT chromosome, start, end FROM vcf");

        List<Row> rowList = rows.collectAsList();

        List<Integer> list = rows.toJavaRDD().groupBy(new Function<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                String key;
                int start = row.getInt(1);
                if (start < 16066000) {
                    key = "1";
                } else if (start < 16067000) {
                    key = "2";
                } else if (start < 16069000) {
                    key = "3";
                } else {
                    key = "4";
                }
                return key;
            }
        }).map(new Function<Tuple2<String, Iterable<Row>>, Integer>() {
            @Override
            public Integer call(Tuple2<String, Iterable<Row>> keyValue) throws Exception {
                System.out.println("key = " + keyValue._1());
                int i = 0;
                PrintWriter writer = new PrintWriter(new File("/tmp/key-" + keyValue._1()));
                java.util.Iterator<Row> iterator = keyValue._2().iterator();
                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    System.out.println("\t\t" + row.get(0) + "\t" + row.get(1));
                    writer.println(row.get(0) + "\t" + row.get(1));
                    i++;
                }
                writer.close();
                return i;
            }
        }).collect();
        list.forEach(i -> System.out.println(i));
    }

//    @Test
    public void streaming00() throws Exception {

        // it doesn't matter what we set to spark's home directory
        SparkConf sparkConf = SparkConfCreator.getConf("AlignmentDatasetTest", "local", 1, true, "");
        System.out.println("sparkConf = " + sparkConf.toDebugString());

//        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(1000));
//        SparkSession sparkSession = new SparkSession(ssc.sparkContext().sc());
        SparkSession sparkSession = new SparkSession(new SparkContext(sparkConf));
        VariantDataset vd = new VariantDataset(sparkSession);
//        Path inputPath = Paths.get("/tmp/test.vcf.avro");
//        Path inputPath = Paths.get("/media/data100/jtarraga/data/spark/100.variants.avro");
//        Path inputPath = Paths.get(getClass().getResource("/100.variants.avro").toURI());
        Path inputPath = Paths.get("/home/jtarraga/appl/hpg-bigdata/hpg-bigdata-core/src/test/resources/100.variants.avro");
        System.out.println(">>>> opening file " + inputPath);
        vd.load(inputPath.toString());
        vd.createOrReplaceTempView("vcf");

        Dataset<Row> rows = vd.sqlContext().sql("SELECT chromosome, start, end FROM vcf");

        List<Row> rowList = rows.collectAsList();


//        rows.grou
//        JavaPairRDD<String, Iterable<Row>> pairs = rows.toJavaRDD().groupBy(new );
        //JavaPairRDD<Character, Iterable<String>>

        List<Integer> list = rows.toJavaRDD().groupBy(new Function<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                String key;
                int start = row.getInt(1);
                if (start < 16066000) {
                    key = "1";
                } else if (start < 16067000) {
                    key = "2";
                } else if (start < 16069000) {
                    key = "3";
                } else {
                    key = "4";
                }
                return key;
            }
        }).map(new Function<Tuple2<String, Iterable<Row>>, Integer>() {
            @Override
            public Integer call(Tuple2<String, Iterable<Row>> keyValue) throws Exception {
                System.out.println("key = " + keyValue._1());
                int i = 0;
                PrintWriter writer = new PrintWriter(new File("/tmp/key-" + keyValue._1()));
                java.util.Iterator<Row> iterator = keyValue._2().iterator();
                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    System.out.println("\t\t" + row.get(0) + "\t" + row.get(1));
                    writer.println(row.get(0) + "\t" + row.get(1));
                    i++;
                }
                writer.close();
//                keyValue._2().forEach(row -> {
//                    i++;
//                    System.out.println("\t\t" + row.get(0) + "\t" + row.get(1))
//                });
                return i;
            }
        }).collect();
        list.forEach(i -> System.out.println(i));
/*
        KeyValueGroupedDataset<String, Row> groups = rows.groupByKey(new MapFunction<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                String key;
                int start = row.getInt(1);
                if (start < 16066000) {
                    key = "1";
                } else if (start < 16067000) {
                    key = "2";
                } else if (start < 16069000) {
                    key = "3";
                } else {
                    key = "4";
                }
                return key;
            }
        }, Encoders.STRING());

//        groups.keys().foreach(k -> System.out.println(k));
/*
        groups.mapGroups(new Function2<String, Iterator<Row>, Object>() {
            @Override
            public Object apply(String s, Iterator<Row> rowIterator) {
                return null;
            }
/*
        r educe(new ReduceFunction<Row>() {
            @Override
            public Row call(Row row, Row t1) throws Exception {
                return null;
            }
        });

        JavaPairRDD<Object, Iterable> keyValues = rows.toJavaRDD().groupBy(new Function<Row, Object>() {
            @Override
            public Object call(Row row) throws Exception {
                return null;
            }
        });

        JavaPairRDD<Object, Iterable> groupMap = productSaleMap.groupBy(new Function<ProductSale, Object>() {
            @Override
            public Object call(ProductSale productSale) throws Exception {
                c.setTime(productSale.getSale().getPurchaseDate());
                return c.get(Calendar.YEAR);
            }
        });


        JavaPairRDD<Object, Long> totalSaleData = groupMap.mapValues(new Function<Iterable, Long>() {
            @Override
            public Long call(Iterable productSales) throws Exception {
                Long sumData = 0L;
                for (ProductSale productSale : productSales) {
                    sumData = sumData + (productSale.getProduct().getPrice() * productSale.getSale().getItemPurchased());
                }
                return sumData;
            }
        });


        JavaPairRDD<String, Row> pairs = rows.toJavaRDD().mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                String key = "";
                int start = row.getInt(1);
                if (start < 16066000) {
                    key = "1";
                } else if (start < 16067000) {
                    key = "2";
                } else if(start < 16069000) {
                    key = "3";
                } else {
                    key = "4";
                }

                return new Tuple2<>(key, row);
            }
        }).reduceByKey()reduce(new Function2<Tuple2<String, Row>, Tuple2<String, Row>, Tuple2<String, Row>>() {
            @Override
            public Tuple2<String, Row> call(Tuple2<String, Row> stringRowTuple2, Tuple2<String, Row> stringRowTuple22) throws Exception {
                return null;
            }
        });

        rows.toJavaRDD((

                JavaPairRDD<String, Integer> rddX =
                x.mapToPair(e -> new Tuple2<String, Integer>(e, 1));

        // New JavaPairRDD
        JavaPairRDD<String, Integer> rddY = rddX.reduceByKey(reduceSumFunc);

        //Print tuples
        for(Tuple2<String, Integer> element : rddY.collect()){
            System.out.println("("+element._1+", "+element._2+")");
        }
    }
}

        rows.toJavaRDD().key
        rows.mreduce(new ReduceFunction<Row>() {
            @Override
            public Row call(Row row, Row t1) throws Exception {
                return null;
            }
        });

/*
        List<Dataset<Row>> buffer = new ArrayList<>();



        //Dataset<Dataset<Row>> datasets;

        //Dataset<Row> dataset;
        //dataset.gr


        Thread thread = new Thread() {
            @Override public void run() {
                System.out.println("------>>>>> Starting thread");
                for (int i = 0; i < 5; i++) {
                    Dataset<Row> ds = vd.sqlContext().sql("SELECT chromosome, start, end FROM vcf");
                    //System.out.println("=================> Stream, dataset " + i + " with " + ds.count() + " rows");
                    System.out.println("------------------------>>>>> adding new dataset");
                    buffer.add(ds);
                }
                System.out.println("------>>>>> Ending thread");
            }
        };

        thread.start();

        while (thread.isAlive() || buffer.size() > 0) {
            System.out.println("Still alive or buffer size " + buffer.size() + "...");
            if (buffer.size() > 0) {
                Dataset<Row> ds = buffer.remove(0);
                List<Row> list = ds.collectAsList();
                for (int i = 0; i < list.size(); i++) {
                    System.out.println(i + "\t" + list.get(i).get(0) + "\t" + list.get(i).get(1) + "\t" + list.get(i).get(2));
                }
            }
            Thread.sleep(1000);
        }





        //StreamingExamples.setStreamingLogLevels();

        // Create the context with a 1 second batch size
        //SparkConf sparkConf = new SparkConf().setAppName("JavaCustomReceiver");
        //JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(1000));

        // Create an input stream with the custom receiver on target ip:port and count the
        // words in input stream of \n delimited text (eg. generated by 'nc')
//        JavaReceiverInputDStream<Dataset<Row>> datasets = ssc.receiverStream(
//                new JavaCustomReceiver(vd.sqlContext()));

 //       JavaDStream<String> words = datasets.map(new Function<Dataset<Row>, String>() {
//            @Override
//            public String call(Dataset dataset) throws Exception {
//                return "toto";
//            }
//        });
//        words.print();
/*
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String x) {
                return Arrays.asList(SPACE.split(x)).iterator();
            }
        });

        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<>(s, 1);
                    }
                }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });

        wordCounts.print();
*///      ssc.start();
  //      ssc.awaitTermination();
    }

//    @Test
    public void map() throws Exception {

        // it doesn't matter what we set to spark's home directory
        SparkConf sparkConf = SparkConfCreator.getConf("AlignmentDatasetTest", "local", 1, true, "");
        System.out.println("sparkConf = " + sparkConf.toDebugString());
        SparkSession sparkSession = new SparkSession(new SparkContext(sparkConf));
        VariantDataset vd;


        vd = new VariantDataset(sparkSession);
        Path inputPath = Paths.get("/tmp/test.vcf.avro");
//        Path inputPath = Paths.get("/media/data100/jtarraga/data/spark/100.variants.avro");
        //Path inputPath = Paths.get(getClass().getResource("/100.variants.avro").toURI());
        System.out.println(">>>> opening file " + inputPath);
        vd.load(inputPath.toString());
        vd.createOrReplaceTempView("vcf");
        //vd.printSchema();

        String sql = "SELECT id, chromosome, start, end, study.studyId, study.samplesData FROM vcf LATERAL VIEW explode (studies) act as study ";
//        String sql = "SELECT id, chromosome, start, end, study.studyId, study.samplesData FROM vcf LATERAL VIEW explode (studies) act as study "
//                + "WHERE (study.studyId = 'hgva@hsapiens_grch37:1000GENOMES_phase_3')";
        Dataset<Row> result = vd.sqlContext().sql(sql);
        result.show();

        SQLContext sqlContext = vd.sqlContext();
        Dataset<String> namesDS = result.map(new MapFunction<Row, String>() {
            @Override
            public String call(Row r) throws Exception {
                File file = new File("/tmp/pos-" + r.get(1) + "_" + r.get(2) + ".txt");
                Dataset<Row> rows = sqlContext.sql("SELECT chromosome, start");
                StringBuilder line = new StringBuilder();
                rows.foreach(row -> {line.append(r.get(1)).append("\t").append(r.get(2)).append("\t")
                        .append(r.get(3)).append("\t").append(r.get(4));});
                PrintWriter writer = new PrintWriter(file);
                writer.println(line);
                writer.close();
                return file.toString();
            }
        }, Encoders.STRING());
        namesDS.show(false);
//                                             }


        //        Encoder<TestResult> testResultEncoder = Encoders.bean(TestResult.class);
//        Dataset<TestResult> namesDS = result.map(new MapFunction<Row, TestResult>() {
//            @Override
//            public TestResult call(Row r) throws Exception {
//                TestResult res = new TestResult();
//                res.setpValue(0.05);
//                res.setMethod("pearson");
//                return res;
////                return r.get(0)
////                        + " : " + ((WrappedArray) r.getList(5).get(0)).head()
////                        + ", " + ((WrappedArray) r.getList(5).get(1)).head();
//            }
//        }, testResultEncoder);
//        namesDS.show(false);

    }

//    @Test
    public void getSamples() throws Exception {

        // it doesn't matter what we set to spark's home directory
        SparkConf sparkConf = SparkConfCreator.getConf("AlignmentDatasetTest", "local", 1, true, "");
        System.out.println("sparkConf = " + sparkConf.toDebugString());
        SparkSession sparkSession = new SparkSession(new SparkContext(sparkConf));
        VariantDataset vd;

//        init();

        if (true) {
            double[] pValues = new double[]{1.0, 0.03, 1.0, 0.38};
            //RowFactory.create("a", Vectors.dense(pValues));
            List<Row> rows = new ArrayList<>();
            for (int i = 0; i < pValues.length; i++) {
                rows.add(RowFactory.create(i, pValues[i]));
            }
            //Arrays.asList(RowFactory.create("a", org.apache.spark.mllib.linalg.Vectors.dense(1.0, 2.0, 3.0)), RowFactory.create("b", org.apache.spark.mllib.linalg.Vectors.dense(4.0, 5.0, 6.0)));
            List<StructField> fields = new ArrayList<>(2);
//            fields.add(DataTypes.createStructField("p", DataTypes.StringType, false));
            fields.add(DataTypes.createStructField("id", DataTypes.IntegerType, false));
            fields.add(DataTypes.createStructField("pValue", DataTypes.DoubleType, false));
            StructType schema = DataTypes.createStructType(fields);
            Dataset<Row> pValueRows = sparkSession.createDataFrame(rows, schema);
            pValueRows.show();
            Dataset<Row> sortedRows = pValueRows.sort(desc("pValue"));
            //sortedRows.withColumn("newId", null);
            sortedRows.show();
            return;
        }


        vd = new VariantDataset(sparkSession);
        Path inputPath = Paths.get("/tmp/test.vcf.avro");
//        Path inputPath = Paths.get("/media/data100/jtarraga/data/spark/100.variants.avro");
        //Path inputPath = Paths.get(getClass().getResource("/100.variants.avro").toURI());
        System.out.println(">>>> opening file " + inputPath);
        vd.load(inputPath.toString());
        vd.createOrReplaceTempView("vcf");
        //vd.printSchema();

        String sql = "SELECT id, chromosome, start, end, study.studyId, study.samplesData FROM vcf LATERAL VIEW explode (studies) act as study ";
//        String sql = "SELECT id, chromosome, start, end, study.studyId, study.samplesData FROM vcf LATERAL VIEW explode (studies) act as study "
//                + "WHERE (study.studyId = 'hgva@hsapiens_grch37:1000GENOMES_phase_3')";
        Dataset<Row> result = vd.sqlContext().sql(sql);
        List list = result.head().getList(5);
        System.out.println("list size = " + list.size());
        System.out.println(((WrappedArray) list.get(0)).head());


        VariantMetadataManager variantMetadataManager = new VariantMetadataManager();
        variantMetadataManager.load(Paths.get(inputPath + ".meta.json"));
        Pedigree pedigree = variantMetadataManager.getPedigree("testing-pedigree");

        ChiSquareAnalysis.run(result, pedigree);

//        //result.show();
//        Encoder<TestResult> testResultEncoder = Encoders.bean(TestResult.class);
//        Dataset<TestResult> namesDS = result.map(new MapFunction<Row, TestResult>() {
//            @Override
//            public TestResult call(Row r) throws Exception {
//                TestResult res = new TestResult();
//                res.setpValue(0.05);
//                res.setMethod("pearson");
//                return res;
////                return r.get(0)
////                        + " : " + ((WrappedArray) r.getList(5).get(0)).head()
////                        + ", " + ((WrappedArray) r.getList(5).get(1)).head();
//            }
//        }, testResultEncoder);
//        namesDS.show(false);


        sparkSession.stop();
    }

    @Test
    public void logistic() throws Exception {
//        init();
//
//        StructType schema = new StructType(new StructField[]{
//                new StructField("label", DataTypes.IntegerType, false, Metadata.empty()),
//                new StructField("features", new VectorUDT(), false, Metadata.empty()),
//        });
//
//        List<Row> data = Arrays.asList(
//                RowFactory.create(0, Vectors.dense(1.0, 2.0)),
//                RowFactory.create(0, Vectors.dense(1.0, 1.0)),
//                RowFactory.create(0, Vectors.dense(1.0, 0.0)),
//                RowFactory.create(1, Vectors.dense(1.0, 1.0)),
//                RowFactory.create(1, Vectors.dense(1.0, 0.0)),
//                RowFactory.create(1, Vectors.dense(1.0, 0.0))
//        );
//        Dataset<Row> dataset = sparkSession.createDataFrame(data, schema);
//        LogisticRegression logistic = new LogisticRegression();
//        LogisticRegressionModel model = logistic.fit(dataset);
//        // Print the coefficients and intercept for logistic regression
//        System.out.println("Coefficients: "
//                + model.coefficients() + " Intercept: " + model.intercept());
//
//        sparkSession.stop();
    }

    //@Test
    public void linear() throws Exception {
//        init();
//
//        StructType schema = new StructType(new StructField[]{
//                new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
//                new StructField("features", new VectorUDT(), false, Metadata.empty()),
//        });
//
//        List<Row> data = Arrays.asList(
//                RowFactory.create(0.0, Vectors.dense(1.0, 2.0)),
//                RowFactory.create(0.0, Vectors.dense(1.0, 1.0)),
//                RowFactory.create(0.0, Vectors.dense(1.0, 0.0)),
//                RowFactory.create(1.0, Vectors.dense(1.0, 1.0)),
//                RowFactory.create(1.0, Vectors.dense(1.0, 0.0)),
//                RowFactory.create(1.0, Vectors.dense(1.0, 0.0))
//        );
//        Dataset<Row> dataset = sparkSession.createDataFrame(data, schema);
//        LinearRegression linear = new LinearRegression();
//        LinearRegressionModel model = linear.fit(dataset);
//        // Print the coefficients and intercept for logistic regression
//        System.out.println("Coefficients: "
//                + model.coefficients() + " Intercept: " + model.intercept());
//
//        sparkSession.stop();
    }

    @Test
    public void chiSquare() throws Exception {

        try {
//            init();
//
//            long count;
//            vd.printSchema();
//
//            vd.show(1);
//            sparkSession.stop();
//
//            //System.exit(0);
//
//
//            Row row = vd.head();
//            int index = row.fieldIndex("studies");
//            System.out.println("studies index = " + index);
//            System.out.println("class at index = " + row.get(index).getClass());
//            List studies = row.getList(index);
//            //List<StudyEntry> studies = row.getList(12);
//            System.out.println(studies.get(0).getClass());
//            GenericRowWithSchema study = (GenericRowWithSchema) studies.get(0);
//            System.out.println("study, field 0 = " + study.getString(0));
//            int sdIndex = study.fieldIndex("samplesData");
//            System.out.println("samplesData index = " + sdIndex);
//            List samplesData = study.getList(sdIndex);
//            for (int i = 0; i < samplesData.size(); i++) {
//                WrappedArray data = (WrappedArray) samplesData.get(i);
//                System.out.println("sample " + i + ": " + data.head());
//            }

//            samplesData.forEach(sd -> System.out.println(((WrappedArray) ((List) sd).get(0)).head()));

//            System.out.println("samplesData size = " + samplesData.size());
//            WrappedArray data = (WrappedArray) samplesData.get(0);
            //           System.out.println("length od data = " + data.length());
            //System.out.println("content = " + data.head());

            //System.out.println(samplesData.get(0).getClass());


//            System.out.println(studies.get(0).getSamplesData().get(0).get(0));

            /*
            for (int i = 0; i < 13; i++) {
                try {
                    System.out.println(i + " ---> " + row.getString(i));
                } catch (Exception e) {
                    System.out.println(i + " ---> ERROR : " + e.getMessage());
                }
            }
*/
            // Create a contingency matrix ((1.0, 2.0), (3.0, 4.0))
//            Matrix mat = Matrices.dense(2, 2, new double[]{1.0, 3.0, 2.0, 4.0});
            // first column (case)    : allele 1: 2.0, allele 2: 0.0
            // second column (control): allele 1: 1.0, allele 2: 1.0
            Matrix mat = Matrices.dense(2, 2, new double[]{2.0, 0.0, 1.0, 1.0});

            // conduct Pearson's independence test on the input contingency matrix
            ChiSqTestResult independenceTestResult = Statistics.chiSqTest(mat);
            // summary of the test including the p-value, degrees of freedom...
            System.out.println(independenceTestResult + "\n");

//            sparkSession.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
