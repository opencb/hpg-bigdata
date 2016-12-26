package org.opencb.hpg.bigdata.tools.variant.analysis.statistics;

import java.security.InvalidParameterException;

public class MultipleTestCorrection {
    public static double[] fdrCorrection(double[] pvalues) throws InvalidParameterException {
        return bHCorrection(pvalues);
    }

    public static double[] bonferroniCorrection(double[] pvalues) throws InvalidParameterException {
        double[] res = new double[pvalues.length];
        int size = pvalues.length;
        for (int i = 0; i < size; i++) {
            res[i] = pvalues[i] * size;
        }
        return StatUtils.pmin(1, res);
    }

    public static double[] bHCorrection(double[] pvalues) throws InvalidParameterException {
        int[] o = ArrayUtils.order(pvalues, true);
        int[] ro = ArrayUtils.order(o);
        double[] mult = ArrayUtils.ordered(pvalues, o);
        for (int i = mult.length, j = 0; i > 0; i--, j++) {
            mult[j] *= ((double) mult.length / (i));
        }
        return ArrayUtils.ordered(StatUtils.pmin(1, StatUtils.cummin(mult)), ro);
    }

    public static double[] hochbergCorrection(double[] pvalues) throws InvalidParameterException {
        int[] o = ArrayUtils.order(pvalues, true);
        int[] ro = ArrayUtils.order(o);
        double[] mult = ArrayUtils.ordered(pvalues, o);
        for (int i = mult.length, j = 0; i > 0; i--, j++) {
            mult[j] *= (mult.length - i + 1);
        }
        return ArrayUtils.ordered(StatUtils.pmin(1, StatUtils.cummin(mult)), ro);
    }

    public static double[] bYCorrection(double[] pvalues) throws InvalidParameterException {
        int[] o = ArrayUtils.order(pvalues, true);
        int[] ro = ArrayUtils.order(o);
        double[] mult = ArrayUtils.ordered(pvalues, o);

        double q = 0.0;
        for (int i = 1; i <= mult.length; i++) {
            q += (double) 1 / i;
        }

        for (int i = mult.length, j = 0; i > 0; i--, j++) {
            mult[j] *= (q * (double) mult.length / (i));
        }
        return ArrayUtils.ordered(StatUtils.pmin(1, StatUtils.cummin(mult)), ro);
    }

    public static double[] holmCorrection(double[] pvalues) throws InvalidParameterException {
        int[] o = ArrayUtils.order(pvalues);
        int[] ro = ArrayUtils.order(o);
        double[] mult = ArrayUtils.ordered(pvalues, o);
        for (int i = 0; i < mult.length; i++) {
            mult[i] *= (mult.length - i);
        }
        return ArrayUtils.ordered(StatUtils.pmin(1, StatUtils.cummax(mult)), ro);
    }
}
