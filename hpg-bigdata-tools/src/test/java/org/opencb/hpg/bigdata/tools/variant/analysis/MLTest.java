package org.opencb.hpg.bigdata.tools.variant.analysis;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.mllib.linalg.Matrices;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.mllib.stat.test.ChiSqTestResult;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import org.opencb.hpg.bigdata.core.lib.SparkConfCreator;
import org.opencb.hpg.bigdata.core.lib.VariantDataset;
import scala.collection.mutable.WrappedArray;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
/**
 * Created by jtarraga on 23/12/16.
 */
public class MLTest {

    SparkConf sparkConf;
    SparkSession sparkSession;
    VariantDataset vd;

    private void init() throws Exception {
        // it doesn't matter what we set to spark's home directory
        sparkConf = SparkConfCreator.getConf("AlignmentDatasetTest", "local", 1, true, "");
        System.out.println("sparkConf = " + sparkConf.toDebugString());
        sparkSession = new SparkSession(new SparkContext(sparkConf));

    }

    //@Test
    public void getSamples() throws Exception {
        init();

        vd = new VariantDataset(sparkSession);
        Path inputPath = Paths.get("/media/data100/jtarraga/data/spark/100.variants.avro");
        //Path inputPath = Paths.get(getClass().getResource("/100.variants.avro").toURI());
        System.out.println(">>>> opening file " + inputPath);
        vd.load(inputPath.toString());
        vd.createOrReplaceTempView("vcf");
        vd.printSchema();

        String sql = "SELECT id, chromosome, start, end, study.studyId, study.samplesData FROM vcf LATERAL VIEW explode (studies) act as study "
                + "WHERE (study.studyId = 'hgva@hsapiens_grch37:1000GENOMES_phase_3')";
        Dataset<Row> result = vd.sqlContext().sql(sql);
        List list = result.head().getList(5);
        System.out.println("list size = " + list.size());
        System.out.println(((WrappedArray) list.get(0)).head());

        result.foreach(r -> System.out.println(r.get(0) + " : " + ((WrappedArray) r.getList(5).get(0)).head()
                + ", " + ((WrappedArray) r.getList(5).get(1)).head()));
        result.show();
        sparkSession.stop();
    }

    @Test
    public void logistic() throws Exception {
        init();

        StructType schema = new StructType(new StructField[]{
                new StructField("label", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("features", new VectorUDT(), false, Metadata.empty()),
        });

        List<Row> data = Arrays.asList(
                RowFactory.create(0, Vectors.dense(1.0, 2.0)),
                RowFactory.create(0, Vectors.dense(1.0, 1.0)),
                RowFactory.create(0, Vectors.dense(1.0, 0.0)),
                RowFactory.create(1, Vectors.dense(1.0, 1.0)),
                RowFactory.create(1, Vectors.dense(1.0, 0.0)),
                RowFactory.create(1, Vectors.dense(1.0, 0.0))
        );
        Dataset<Row> dataset = sparkSession.createDataFrame(data, schema);
        LogisticRegression logistic = new LogisticRegression();
        LogisticRegressionModel model = logistic.fit(dataset);
        // Print the coefficients and intercept for logistic regression
        System.out.println("Coefficients: "
                + model.coefficients() + " Intercept: " + model.intercept());

        sparkSession.stop();
    }

    //@Test
    public void linear() throws Exception {
        init();

        StructType schema = new StructType(new StructField[]{
                new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("features", new VectorUDT(), false, Metadata.empty()),
        });

        List<Row> data = Arrays.asList(
                RowFactory.create(0.0, Vectors.dense(1.0, 2.0)),
                RowFactory.create(0.0, Vectors.dense(1.0, 1.0)),
                RowFactory.create(0.0, Vectors.dense(1.0, 0.0)),
                RowFactory.create(1.0, Vectors.dense(1.0, 1.0)),
                RowFactory.create(1.0, Vectors.dense(1.0, 0.0)),
                RowFactory.create(1.0, Vectors.dense(1.0, 0.0))
        );
        Dataset<Row> dataset = sparkSession.createDataFrame(data, schema);
        LinearRegression linear = new LinearRegression();
        LinearRegressionModel model = linear.fit(dataset);
        // Print the coefficients and intercept for logistic regression
        System.out.println("Coefficients: "
                + model.coefficients() + " Intercept: " + model.intercept());

        sparkSession.stop();
    }

//    @Test
    public void chiSquare() throws Exception {

        try {
            init();

            long count;
            vd.printSchema();

            vd.show(1);
            sparkSession.stop();

            System.exit(0);


            Row row = vd.head();
            int index = row.fieldIndex("studies");
            System.out.println("studies index = " + index);
            System.out.println("class at index = " + row.get(index).getClass());
            List studies = row.getList(index);
            //List<StudyEntry> studies = row.getList(12);
            System.out.println(studies.get(0).getClass());
            GenericRowWithSchema study = (GenericRowWithSchema) studies.get(0);
            System.out.println("study, field 0 = " + study.getString(0));
            int sdIndex = study.fieldIndex("samplesData");
            System.out.println("samplesData index = " + sdIndex);
            List samplesData = study.getList(sdIndex);
            for (int i = 0; i < samplesData.size(); i++) {
                WrappedArray data = (WrappedArray) samplesData.get(i);
                System.out.println("sample " + i + ": " + data.head());
            }

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

            sparkSession.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
