package org.opencb.hpg.bigdata.analysis.variant.statistics;

public class StatsUtils {
    /**
     * Returns a vector whose elements are the cumulative sums.
     *
     * @param   data    Input vector
     * @return  Output  vector
     */
    public static double[] cummin(int [] data) {
        return cummin(toDoubleArray(data));
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
        return cummax(toDoubleArray(data));
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

    public static double[] toDoubleArray(int[] data) {
        double[] res = null;
        if (data != null) {
            res = new double[data.length];
            for (int i = 0; i < data.length; i++) {
                res[i] = (double) data[i];
            }
        }
        return res;
    }

    public static int[] order(int[] data) {
        return order(toDoubleArray(data), false);
    }

    public static int[] order(double[] data) {
        return order(data, false);
    }

    public static int[] order(double[] data, boolean decreasing) {
        int[] order = new int[data.length];
        double[] clonedData = data.clone();

        if (decreasing) {
            double min = nonInfinityAndMinValueMin(clonedData);
            // to avoid the NaN missorder
            for (int i = 0; i < clonedData.length; i++) {
                if (Double.isNaN(clonedData[i])) {
                    clonedData[i] = min - 3;
                }
                if (clonedData[i] == Double.NEGATIVE_INFINITY) {
                    clonedData[i] = min - 2;
                }
                if (clonedData[i] == Double.MIN_VALUE) {
                    clonedData[i] = min - 1;
                }
            }
            // get the order
            for (int i = 0; i < clonedData.length; i++) {
                order[i] = maxIndex(clonedData);
                clonedData[order[i]] = Double.NEGATIVE_INFINITY;
            }

        } else {
            double max = nonInfinityAndMaxValueMax(clonedData);
            // to avoid the NaN missorder
            for (int i = 0; i < clonedData.length; i++) {
                if (Double.isNaN(clonedData[i])) {
                    clonedData[i] = max + 3;
                }
                if (clonedData[i] == Double.POSITIVE_INFINITY) {
                    clonedData[i] = max + 2;
                }
                if (clonedData[i] == Double.MAX_VALUE) {
                    clonedData[i] = max + 1;
                }
            }
            // get the order
            for (int i = 0; i < clonedData.length; i++) {
                order[i] = minIndex(clonedData);
                clonedData[order[i]] = Double.POSITIVE_INFINITY;
            }
        }

        return order;
    }

    public static int minIndex(double[] values) {
        return minIndex(values, 0, values.length);
    }

    public static int minIndex(double[] values, int begin, int end) {
        double min = Double.NaN;
        int index = -1;
        min = values[begin];
        index = begin;
        for (int i = begin; i < end; i++) {
            if (!Double.isNaN(values[i])) {
                if (values[i] < min) {
                    min = values[i];
                    index = i;
                }
            }
        }
        return index;
    }

    public static int maxIndex(double[] values) {
        return maxIndex(values, 0, values.length);
    }

    public static int maxIndex(double[] values, int begin, int end) {
        double max = Double.NaN;
        int index = -1;
        max = values[begin];
        index = begin;
        for (int i = begin; i < end; i++) {
            if (!Double.isNaN(values[i])) {
                if (values[i] > max) {
                    max = values[i];
                    index = i;
                }
            }
        }
        return index;
    }

    public static double nonInfinityAndMinValueMin(double[] data) {
        double min = Double.POSITIVE_INFINITY;
        for (int i = 0; i < data.length; i++) {
            if (data[i] < min && data[i] != Double.NEGATIVE_INFINITY && data[i] != Double.MIN_VALUE) {
                min = data[i];
            }
        }
        return min;
    }

    public static double nonInfinityAndMaxValueMax(double[] data) {
        double max = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < data.length; i++) {
            if (data[i] > max && data[i] != Double.POSITIVE_INFINITY && data[i] != Double.MAX_VALUE) {
                max = data[i];
            }
        }
        return max;
    }

    public static double[] ordered(final double[] data, int[] pos) {
        double[] ordered = null;
        if (data.length == pos.length) {
            ordered = new double[data.length];
            for (int i = 0; i < pos.length; i++) {
                ordered[i] = data[pos[i]];
            }
        }
        return ordered;
    }
}
