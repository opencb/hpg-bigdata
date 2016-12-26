package org.opencb.hpg.bigdata.tools.variant.analysis.statistics;

public class StatUtils {
    /**
     * Returns a vector whose elements are the cumulative sums.
     *
     * @param   data    Input vector
     * @return  Output  vector
     */
    public static double[] cummin(int [] data) {
        return cummin(ArrayUtils.toDoubleArray(data));
    }

    /**
     * Returns a vector whose elements are the cumulative sums.
     *
     * @param   data    Input vector
     * @return  Output  vector
     */
    public static double[] cummin(double [] data) {
        double[] res = new double[data.length];
        double min = Double.POSITIVE_INFINITY;
        for (int i = 0; i < data.length; i++) {
            if (Double.isNaN(data[i]) || Double.isNaN(min)) {
                min = min + data[i];  /* propagate NA and NaN */
            } else {
                min = (min < data[i]) ? min : data[i];
            }
            res[i] = min;
        }
        return res;
    }

    /**
     * Returns a vector whose elements are the cumulative sums.
     *
     * @param   data    Input vector
     * @return  Output  vector
     */
    public static double[] cummax(int [] data) {
        return cummax(ArrayUtils.toDoubleArray(data));
    }

    /**
     * Returns a vector whose elements are the cumulative sums.
     *
     * @param   data    Input vector
     * @return  Output  vector
     */
    public static double[] cummax(double [] data) {
        double[] res = new double[data.length];
        double max = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < data.length; i++) {
            if (Double.isNaN(data[i]) || Double.isNaN(max)) {
                max = max + data[i];  /* propagate NA and NaN */
            } else {
                max = (max > data[i]) ? max : data[i];
            }
            res[i] = max;
        }
        return res;
    }

    /**
     * Returns a vector whose elements are the cumulative sums.
     *
     * @param   data    Input vector
     * @return  Output  vector
     */
    public static double[] cumsum(double[] data) {
        double[] res = new double[data.length];
        double sum = 0;
        for (int i = 0; i < data.length; i++) {
            sum += data[i];
            res[i] = sum;
        }
        return res;
    }

    /**
     * Returns the (parallel) minima of the input values.
     *
     * @param   min     Minimum value
     * @param   data    Input vector
     * @return          Output vector
     */
    public static double[] pmin(double min, double [] data) {
        double[] res = new double[data.length];
        for (int i = 0; i < data.length; i++) {
            res[i] = (min < data[i]) ? min : data[i];
        }
        return res;
    }

    /**
     * Returns the (parallel) minima of the input values.
     *
     * @param   max     Maximum value
     * @param   data    Input vector
     * @return          Output vector
     */
    public static double[] pmax(double max, double [] data) {
        double[] res = new double[data.length];
        for (int i = 0; i < data.length; i++) {
            res[i] = (max > data[i]) ? max : data[i];
        }
        return res;
    }
}
