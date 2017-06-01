package org.opencb.hpg.bigdata.analysis.variant;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.opencb.hpg.bigdata.analysis.Analysis;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jtarraga on 30/05/17.
 */
public class LinearRegressionAnalysis extends Analysis {

    private String depVarName;
    private String indepVarName;

    private int numIterations = 10; // number of iterations
    private double regularization = 0.3; // regularization parameter
    private double elasticNet = 0.8; // elastic net mixing parameter

    @Override
    public void run() {
        LinearRegression lr = new LinearRegression()
                .setMaxIter(numIterations)
                .setRegParam(regularization)
                .setElasticNetParam(elasticNet);

        // prepare dataset
        int numFeatures = 10;
        double target = Double.NaN;
        double[] features = new double[numFeatures];
        LabeledPoint lp = new LabeledPoint(target, Vectors.dense(features));

        List<LabeledPoint> list = new ArrayList<LabeledPoint>();
        list.add(lp);
        JavaSparkContext jsc = new JavaSparkContext();
        SQLContext sqlContext = new SQLContext(jsc);
        JavaRDD<LabeledPoint> data = jsc.parallelize(list);
        data.cache();

        // fit the model
        Dataset<Row> training = sqlContext.createDataFrame(data.rdd(), LabeledPoint.class);
        LinearRegressionModel lrModel = lr.fit(training);

        // print the coefficients and intercept for linear regression
        System.out.println("Coefficients: "
                + lrModel.coefficients() + " Intercept: " + lrModel.intercept());

        // summarize the model over the training set and print out some metrics
        LinearRegressionTrainingSummary trainingSummary = lrModel.summary();
        System.out.println("numIterations: " + trainingSummary.totalIterations());
        System.out.println("objectiveHistory: " + Vectors.dense(trainingSummary.objectiveHistory()));
        trainingSummary.residuals().show();
        System.out.println("RMSE: " + trainingSummary.rootMeanSquaredError());
        System.out.println("r2: " + trainingSummary.r2());
    }

    public LinearRegressionAnalysis(String datasetName, String studyName, String depVarName, String indepVarName) {
        this(datasetName, studyName, depVarName, indepVarName, 10, 0.3, 0.8);
    }

    public LinearRegressionAnalysis(String datasetName, String studyName, String depVarName, String indepVarName,
                                    int numIterations, double regularization, double elasticNet) {
        this.datasetName = datasetName;
        this.studyName = studyName;
        this.depVarName = depVarName;
        this.indepVarName = indepVarName;
        this.numIterations = numIterations;
        this.regularization = regularization;
        this.elasticNet = elasticNet;
    }

    public String getDepVarName() {
        return depVarName;
    }

    public void setDepVarName(String depVarName) {
        this.depVarName = depVarName;
    }

    public String getIndepVarName() {
        return indepVarName;
    }

    public void setIndepVarName(String indepVarName) {
        this.indepVarName = indepVarName;
    }

    public int getNumIterations() {
        return numIterations;
    }

    public void setNumIterations(int numIterations) {
        this.numIterations = numIterations;
    }

    public double getRegularization() {
        return regularization;
    }

    public void setRegularization(double regularization) {
        this.regularization = regularization;
    }

    public double getElasticNet() {
        return elasticNet;
    }

    public void setElasticNet(double elasticNet) {
        this.elasticNet = elasticNet;
    }
}
