package org.opencb.hpg.bigdata.tools.variant.analysis;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.BlockRealMatrix;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by jtarraga on 19/12/16.
 */
public class CMCTest {

    // CMC(phenotype, genotype, maf=0.05, perm=500)
    // CMC: Combined Multivariate and Collapsing Method
    //
    // Info:
    //   cases  controls  variants   rarevar       maf      perm     cmc.stat   asym.pval   perm.pval
    //       5         5         8         6      0.05       500     2.666667    0.602539    0.346000

    @Test
    public void test() {
        double[] phenotype = {1, 1, 1, 1, 1, 0, 0, 0, 0, 0};
        double[][] genotype = {
                {0, 0, 0, 0, 0, 0, 0, 0},
                {0, 0, 0, 0, 0, 0, 0, 0},
                {0, 0, 0, 0, 0, 0, 0, 0},
                {0, 0, 0, 0, 0, 0, 0, 0},
                {0, 0, 0, 0, 0, 0, 0, 0},
                {0, 0, 0, 0, 0, 0, 0, 0},
                {0, 0, 0, 0, 0, 0, 1, 0},
                {0, 0, 0, 0, 0, 0, 0, 0},
                {0, 0, 0, 0, 0, 0, 0, 0},
                {1, 0, 0, 0, 0, 0, 0, 0}
        };

        CMC cmc = new CMC();
        CMC.Result result = cmc.run(new ArrayRealVector(phenotype), new BlockRealMatrix(genotype));
        System.out.println(result.toString());

        assertEquals("CMC statistic does not match", result.getStatistic(), 2.666666666666665, 0.1);
    }
}