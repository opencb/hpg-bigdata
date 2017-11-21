package org.opencb.hpg.bigdata.analysis.variant;

import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.mllib.stat.test.ChiSqTestResult;

/**
 * Created by jtarraga on 21/12/16.
 */
public class ChiSquare {

    public ChiSquare() {
    }

    public ChiSqTestResult run(Matrix matrix) {
        // conduct Pearson's independence test on the input contingency matrix
        return Statistics.chiSqTest(matrix);
    }
}
