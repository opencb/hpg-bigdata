package org.opencb.hpg.bigdata.tools.variant.analysis.statistics;

import java.security.InvalidParameterException;

public class FisherExactTest {
    public static final int LESS = 1;
    public static final int GREATER = 2; //list 1 Vs Genome
    public static final int TWO_SIDED = 3;

    private double[] factorialLogarithms;

    private void initLogarithmArray(int nmax) {
        factorialLogarithms = new double[nmax];
        factorialLogarithms[0] = 0;
        factorialLogarithms[1] = 0;
        for (int i = 2; i < nmax; i++) {
            factorialLogarithms[i] = factorialLogarithms[i - 1] + Math.log(i);
        }
    }

    public FisherTestResult fisherTest(int a, int b, int c, int d) {
        return fisherTest(a, b, c, d, TWO_SIDED);
    }

    public FisherTestResult fisherTest(int[] values) throws InvalidParameterException {
        if (values == null || values.length != 4) {
            throw new InvalidParameterException("The matrix must have 4 columns (a, b, c and d)");
        }
        return fisherTest(values[0], values[1], values[2], values[3]);
    }

    public FisherTestResult fisherTest(int a, int b, int c, int d, int mode) {
        initLogarithmArray(a + b + c + d + 1);
        return new FisherTestResult(computeFisherTest(a, b, c, d, mode), computeOddRatio(a, b, c, d));
    }

    private double computeFisherTest(int a, int b, int c, int d, int mode) {
        int iniA = 0, iniB = 0, iniC = 0, iniD = 0;
        double result;
        int steps = 0;
        switch (mode) {
            case 1:
                if (a > d) {
                    iniA = a - d;
                    iniB = b + d;
                    iniC = c + d;
                    iniD = 0;
                    steps = d;
                } else {
                    iniA = 0;
                    iniB = b + a;
                    iniC = c + a;
                    iniD = d - a;
                    steps = a;
                }
                break;
            case 2:
                iniA = a;
                iniB = b;
                iniC = c;
                iniD = d;
                if (b > c) {
                    steps = c;
                } else {
                    steps = b;
                }
                break;
            case 3:
                if (a > d) {
                    iniA = a - d;
                    iniB = b + d;
                    iniC = c + d;
                    iniD = 0;
                    steps = d;
                } else {
                    iniA = 0;
                    iniB = b + a;
                    iniC = c + a;
                    iniD = d - a;
                    steps = a;
                }
                if (b > c) {
                    steps += c;
                } else {
                    steps += b;
                }
                break;
            default: {
                break;
            }
        }

        double n = factorialLogarithms[a + b + c + d];
        double num = factorialLogarithms[iniA + iniB] + factorialLogarithms[iniB + iniD]
                + factorialLogarithms[iniA + iniC] + factorialLogarithms[iniC + iniD];
        double den = n + factorialLogarithms[iniA] + factorialLogarithms[iniB] + factorialLogarithms[iniC]
                + factorialLogarithms[iniD];
        double pAct = num - den;

        if (mode == 3) {
            num = factorialLogarithms[a + b] + factorialLogarithms[b + d] + factorialLogarithms[a + c]
                    + factorialLogarithms[c + d];
            den = n + factorialLogarithms[a] + factorialLogarithms[b] + factorialLogarithms[c] + factorialLogarithms[d];
            double pIni = num - den;

            if (pAct <= pIni) {
                result = Math.exp(pAct);
            } else {
                result = 0;
            }

            while (steps-- > 0) {
                iniA++;
                iniD++;
                iniC--;
                iniB--;
                num = factorialLogarithms[iniA + iniB] + factorialLogarithms[iniB + iniD]
                        + factorialLogarithms[iniA + iniC] + factorialLogarithms[iniC + iniD];
                den = n + factorialLogarithms[iniA] + factorialLogarithms[iniB] + factorialLogarithms[iniC]
                        + factorialLogarithms[iniD];
                pAct = num - den;
                if (pAct <= pIni) {
                    result += Math.exp(pAct);
                }
            }
        } else {
            result = Math.exp(pAct);
            while (steps-- > 0) {
                iniA++;
                iniD++;
                iniC--;
                iniB--;
                num = factorialLogarithms[iniA + iniB] + factorialLogarithms[iniB + iniD]
                        + factorialLogarithms[iniA + iniC] + factorialLogarithms[iniC + iniD];
                den = n + factorialLogarithms[iniA] + factorialLogarithms[iniB] + factorialLogarithms[iniC]
                        + factorialLogarithms[iniD];
                pAct = num - den;
                result += Math.exp(pAct);
            }
        }
        return result;
    }

    private double computeOddRatio(int a, int b, int c, int d) {
        return (double) (a * d) / (b * c);
    }
}
