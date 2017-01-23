package org.opencb.hpg.bigdata.tools.variant.analysis;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opencb.hpg.bigdata.core.lib.SparkConfCreator;

import java.util.Arrays;
import java.util.List;

/**
 * Created by jtarraga on 21/12/16.
 */
public class ChiSquareTest {

    static SparkConf sparkConf;
    static SparkSession sparkSession;

    @BeforeClass
    public static void setup() {
        sparkConf = SparkConfCreator.getConf("MyTest", "local", 1, true, "/home/joaquin/softs/spark-2.0.0-bin-hadoop2.7/bin");

        System.out.println("sparkConf = " + sparkConf.toDebugString());
        SparkContext sc = new SparkContext(sparkConf);
        sc.setLogLevel("FATAL");
        sparkSession = new SparkSession(sc);
    }

    @AfterClass
    public static void shutdown() {
        sparkSession.sparkContext().stop();
    }

    @Test
    public void test() {
//        // create a contingency matrix ((3.0, 4.0), (5.0, 6.0))
//        Matrix matrix = Matrices.dense(2, 2, new double[]{3.0, 5.0, 4.0, 6.0});
//
//        // chi square
//        ChiSquare chiSquare = new ChiSquare();
//        ChiSqTestResult result = chiSquare.run(matrix);
//        // summary of the test including the p-value, degrees of freedom..
//        System.out.println(result + "\n");

        List<Row> data = Arrays.asList(
                RowFactory.create(0.0, Vectors.dense(3.0, 4.0)),
                RowFactory.create(1.0, Vectors.dense(5.0, 6.0)));
        StructType schema = new StructType(new StructField[]{
                new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("features", new VectorUDT(), false, Metadata.empty()),
        });
        Dataset<Row> dataset = sparkSession.createDataFrame(data, schema);

        // logistic
        LogisticRegression logistic = new LogisticRegression();
        LogisticRegressionModel logisticModel = logistic.fit(dataset);
        // print the coefficients and intercept for linear regression.
        System.out.println("Coefficients: "
                + logisticModel.coefficients() + " Intercept: " + logisticModel.intercept());


        // linear
        LinearRegression linear = new LinearRegression();
        // Fit the model.
        LinearRegressionModel linearModel = linear.fit(dataset);
        // print the coefficients and intercept for linear regression.
        System.out.println("Coefficients: "
                + linearModel.coefficients() + " Intercept: " + linearModel.intercept());
        // summarize the model over the training set and print out some metrics.
        LinearRegressionTrainingSummary trainingSummary = linearModel.summary();
        System.out.println("numIterations: " + trainingSummary.totalIterations());
        System.out.println("objectiveHistory: " + Vectors.dense(trainingSummary.objectiveHistory()));
        trainingSummary.residuals().show();
        System.out.println("RMSE: " + trainingSummary.rootMeanSquaredError());
        System.out.println("r2: " + trainingSummary.r2());
    }
}