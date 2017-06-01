package org.opencb.hpg.bigdata.analysis.variant;

import scala.Serializable;

/**
 * Created by jtarraga on 11/01/17.
 */
public class ChiSquareAnalysisResult implements Serializable {
    private double statistic;
    private double pValue;
    private double adjPValue;
    private int degreesOfFreedom;
    private String method;

    public double getAdjPValue() {
        return adjPValue;
    }

    public void setAdjPValue(double adjPValue) {
        this.adjPValue = adjPValue;
    }

    public int getDegreesOfFreedom() {
        return degreesOfFreedom;
    }

    public void setDegreesOfFreedom(int degreesOfFreedom) {
        this.degreesOfFreedom = degreesOfFreedom;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public double getpValue() {
        return pValue;
    }

    public void setpValue(double pValue) {
        this.pValue = pValue;
    }

    public double getStatistic() {
        return statistic;
    }

    public void setStatistic(double statistic) {
        this.statistic = statistic;
    }
}
