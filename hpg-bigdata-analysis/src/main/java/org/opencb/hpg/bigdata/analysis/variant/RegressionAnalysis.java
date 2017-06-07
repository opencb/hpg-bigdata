package org.opencb.hpg.bigdata.analysis.variant;

import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.opencb.hpg.bigdata.analysis.AnalysisExecutorException;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jtarraga on 07/06/17.
 */
public class RegressionAnalysis extends VariantAnalysisExecutor {
    protected static int defaultNumIterations = 100; // number of iterations
    protected static double defaultRegularization = 0.0; // regularization parameter
    protected static double defaultElasticNet = 0.0; // elastic net mixing parameter

    protected String depVarName;
    protected List<Double> depVarValues;
    protected String indepVarName;
    protected List<Double> indepVarValues;

    protected int numIterations = defaultNumIterations;
    protected double regularization = defaultRegularization;
    protected double elasticNet = defaultElasticNet;

    @Override
    public void execute() throws AnalysisExecutorException {
    }

    public void execute(List<Double> depVarValues, List<Double> indepVarValues) throws AnalysisExecutorException {
        this.depVarValues = depVarValues;
        this.indepVarValues = indepVarValues;

        execute();
    }

    protected Dataset<Row> createTrainingDataset() {
        List<LabeledPoint> list = new ArrayList();
        for (int i = 0; i < indepVarValues.size(); i++) {
            list.add(new LabeledPoint(depVarValues.get(i), new DenseVector(new double[]{0, indepVarValues.get(i)})));
        }
        return sparkSession.createDataFrame(list, LabeledPoint.class);
    }

    public RegressionAnalysis(String datasetName, String studyName, String depVarName, String indepVarName,
                              SparkSession sparkSession) {
        this(datasetName, studyName, depVarName, indepVarName, defaultNumIterations, defaultRegularization,
                defaultElasticNet, sparkSession);
    }

    public RegressionAnalysis(String datasetName, String studyName, String depVarName, String indepVarName,
                              int numIterations, double regularization, double elasticNet, SparkSession sparkSession) {
        this.datasetName = datasetName;
        this.studyName = studyName;
        this.depVarName = depVarName;
        this.indepVarName = indepVarName;
        this.numIterations = numIterations;
        this.regularization = regularization;
        this.elasticNet = elasticNet;
        this.sparkSession = sparkSession;
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

    public List<Double> getDepVarValues() {
        return depVarValues;
    }

    public void setDepVarValues(List<Double> depVarValues) {
        this.depVarValues = depVarValues;
    }

    public List<Double> getIndepVarValues() {
        return indepVarValues;
    }

    public void setIndepVarValues(List<Double> indepVarValues) {
        this.indepVarValues = indepVarValues;
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
