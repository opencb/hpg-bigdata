package org.opencb.hpg.bigdata.tools.variant.analysis.statistics;

public class FisherTestResult {

    private double oddRatio;
    private double pValue;

    public FisherTestResult(double pValue, double oddRatio) {
        this.pValue = pValue;
        this.oddRatio = oddRatio;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(pValue).append("\t").append(oddRatio).append("\t");
        return sb.toString();
    }

    public void setOddRatio(double oddRatio) {
        this.oddRatio = oddRatio;
    }
    public double getOddRatio() {
        return oddRatio;
    }
    public void setPValue(double pValue) {
        this.pValue = pValue;
    }
    public double getPValue() {
        return pValue;
    }
}
