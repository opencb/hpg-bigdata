package org.opencb.hpg.bigdata.analysis.variant;

import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by jtarraga on 30/05/17.
 */
public class LinearRegressionAnalysis extends RegressionAnalysis {
    private LinearRegression linearRegression;

    @Override
    public void execute() {
        // create dataset
        Dataset<Row> training = createTrainingDataset();

        try {
            // fit the model
            LinearRegressionModel lrModel = linearRegression.fit(training);

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
        } catch (Exception e) {
            System.out.println("ERROR: computing LinearRegressionModel: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public LinearRegressionAnalysis(String datasetName, String studyName, String depVarName, String indepVarName,
                                    SparkSession sparkSession) {
        this(datasetName, studyName, depVarName, indepVarName, defaultNumIterations, defaultRegularization,
                defaultElasticNet, sparkSession);
    }

    public LinearRegressionAnalysis(String datasetName, String studyName, String depVarName, String indepVarName,
                                    int numIterations, double regularization, double elasticNet, SparkSession sparkSession) {
        super(datasetName, studyName, depVarName, indepVarName, numIterations, regularization, elasticNet, sparkSession);

        linearRegression = new LinearRegression().setMaxIter(numIterations).setRegParam(regularization)
                .setElasticNetParam(elasticNet);
    }
}
