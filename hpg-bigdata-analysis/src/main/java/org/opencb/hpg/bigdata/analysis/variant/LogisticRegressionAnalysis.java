package org.opencb.hpg.bigdata.analysis.variant;

import org.apache.spark.ml.classification.BinaryLogisticRegressionSummary;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.classification.LogisticRegressionTrainingSummary;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/**
 * Created by jtarraga on 30/05/17.
 */
public class LogisticRegressionAnalysis extends RegressionAnalysis {

    private LogisticRegression logisticRegression;

    @Override
    public void execute() {
        // create dataset
        Dataset<Row> training = createTrainingDataset();

        // fit the model
        LogisticRegressionModel lrModel = logisticRegression.fit(training);

        // print the coefficients and intercept for linear regression
        System.out.println("Coefficients: "
                + lrModel.coefficients() + " Intercept: " + lrModel.intercept());
        System.out.println("OR (for coeff.) = " + Math.exp(lrModel.coefficients().apply(1)));

        // summarize the model over the training set and print out some metrics
        LogisticRegressionTrainingSummary trainingSummary = lrModel.summary();
        System.out.println("numIterations: " + trainingSummary.totalIterations());
        System.out.println("objectiveHistory: " + Vectors.dense(trainingSummary.objectiveHistory()));

        // obtain the loss per iteration
        double[] objectiveHistory = trainingSummary.objectiveHistory();
        for (double lossPerIteration : objectiveHistory) {
            System.out.println(lossPerIteration);
        }

        // obtain the metrics useful to judge performance on test data
        // we cast the summary to a BinaryLogisticRegressionSummary since the problem is a binary
        // classification problem.
        BinaryLogisticRegressionSummary binarySummary =
                (BinaryLogisticRegressionSummary) trainingSummary;

        // obtain the receiver-operating characteristic as a dataframe and areaUnderROC
        Dataset<Row> roc = binarySummary.roc();
        roc.show();
        roc.select("FPR").show();
        System.out.println(binarySummary.areaUnderROC());

        // get the threshold corresponding to the maximum F-Measure and rerun LogisticRegression with
        // this selected threshold
        Dataset<Row> fMeasure = binarySummary.fMeasureByThreshold();
        double maxFMeasure = fMeasure.select(functions.max("F-Measure")).head().getDouble(0);
        double bestThreshold = fMeasure.where(fMeasure.col("F-Measure").equalTo(maxFMeasure))
                .select("threshold").head().getDouble(0);
        lrModel.setThreshold(bestThreshold);
    }

    public LogisticRegressionAnalysis(String datasetName, String studyName, String depVarName, String indepVarName,
                                      SparkSession sparkSession) {
        this(datasetName, studyName, depVarName, indepVarName, defaultNumIterations, defaultRegularization,
                defaultElasticNet, sparkSession);
    }

    public LogisticRegressionAnalysis(String datasetName, String studyName, String depVarName, String indepVarName,
                                      int numIterations, double regularization, double elasticNet,
                                      SparkSession sparkSession) {
        super(datasetName, studyName, depVarName, indepVarName, numIterations, regularization, elasticNet, sparkSession);

        this.logisticRegression = new LogisticRegression().setMaxIter(numIterations).setRegParam(regularization)
                .setElasticNetParam(elasticNet);
    }
}
