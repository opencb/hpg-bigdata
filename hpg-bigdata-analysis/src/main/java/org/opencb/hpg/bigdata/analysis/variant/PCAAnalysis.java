package org.opencb.hpg.bigdata.analysis.variant;

import org.apache.spark.ml.feature.PCA;
import org.apache.spark.ml.feature.PCAModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Created by jtarraga on 30/05/17.
 */
public class PCAAnalysis extends VariantAnalysisExecutor {

    private int kValue = 3;
    private String featureName;

    @Override
    public void execute() {
        // prepare dataset
        Dataset<Row> dataset = null;

        // fit PCA
        PCAModel pca = new PCA()
                .setInputCol(featureName)
                .setOutputCol("pca")
                .setK(kValue)
                .fit(dataset);

        Dataset<Row> result = pca.transform(dataset).select("pca");
        result.show(false);
    }

    public PCAAnalysis(String datasetName, String studyName, String featureName) {
        this(datasetName, studyName, featureName, 3);
    }

    public PCAAnalysis(String datasetName, String studyName, String featureName, int kValue) {
        this.datasetName = datasetName;
        this.studyName = studyName;
        this.featureName = featureName;
        this.kValue = kValue;
    }

    public int getkValue() {
        return kValue;
    }

    public void setkValue(int kValue) {
        this.kValue = kValue;
    }

    public String getFeatureName() {
        return featureName;
    }

    public void setFeatureName(String featureName) {
        this.featureName = featureName;
    }
}
