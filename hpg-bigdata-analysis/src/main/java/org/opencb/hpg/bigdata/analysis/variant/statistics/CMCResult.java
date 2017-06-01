package org.opencb.hpg.bigdata.analysis.variant.statistics;

import scala.collection.mutable.StringBuilder;

/**
 * Created by jtarraga on 26/12/16.
 */
public class CMCResult {
    private int numCases;
    private int numControls;
    private int numVariants;
    private int numRareVariants;
    private double maf;
    private int numPermutations;
    private double statistic;
    private double asymPvalue;
    private double permPvalue;

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("CMC: Combined Multivariate and Collapsing Method\n");
        sb.append("Info:\n");
        sb.append("\tcases: " + getNumCases() + "\n");
        sb.append("\tcontrols: " + getNumControls() + "\n");
        sb.append("\tvariants: " + getNumVariants() + "\n");
        sb.append("\trare variants: " + getNumRareVariants() + "\n");
        sb.append("\tmaf: " + getMaf() + "\n");
        sb.append("\tpermutations: " + getNumPermutations() + "\n");
        sb.append("\tstatistic: " + getStatistic() + "\n");
        sb.append("\tasymptotic p-value: " + getAsymPvalue() + "\n");
        sb.append("\tpermuted p-value: " + getPermPvalue() + "\n");
        return sb.toString();
    }

    public int getNumCases() {
        return numCases;
    }

    public void setNumCases(int numCases) {
        this.numCases = numCases;
    }

    public int getNumControls() {
        return numControls;
    }

    public void setNumControls(int numControls) {
        this.numControls = numControls;
    }

    public int getNumVariants() {
        return numVariants;
    }

    public void setNumVariants(int numVariants) {
        this.numVariants = numVariants;
    }

    public int getNumRareVariants() {
        return numRareVariants;
    }

    public void setNumRareVariants(int numRareVariants) {
        this.numRareVariants = numRareVariants;
    }

    public double getMaf() {
        return maf;
    }

    public void setMaf(double maf) {
        this.maf = maf;
    }

    public int getNumPermutations() {
        return numPermutations;
    }

    public void setNumPermutations(int numPermutations) {
        this.numPermutations = numPermutations;
    }

    public double getStatistic() {
        return statistic;
    }

    public void setStatistic(double statistic) {
        this.statistic = statistic;
    }

    public double getAsymPvalue() {
        return asymPvalue;
    }

    public void setAsymPvalue(double asymPvalue) {
        this.asymPvalue = asymPvalue;
    }

    public double getPermPvalue() {
        return permPvalue;
    }

    public void setPermPvalue(double permPvalue) {
        this.permPvalue = permPvalue;
    }
}
