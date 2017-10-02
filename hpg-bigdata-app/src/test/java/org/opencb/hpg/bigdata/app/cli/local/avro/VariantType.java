/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package org.opencb.hpg.bigdata.app.cli.local.avro;
@SuppressWarnings("all")
/** * Type of variation, which depends mostly on its length.
     * <ul>
     * <li>SNVs involve a single nucleotide, without changes in length</li>
     * <li>MNVs involve multiple nucleotides, without changes in length</li>
     * <li>Indels are insertions or deletions of less than SV_THRESHOLD (50) nucleotides</li>
     * <li>Structural variations are large changes of more than SV_THRESHOLD nucleotides</li>
     * <li>Copy-number variations alter the number of copies of a region</li>
     * </ul> */
public enum VariantType {
  SNV, SNP, MNV, MNP, INDEL, SV, INSERTION, DELETION, TRANSLOCATION, INVERSION, CNV, DUPLICATION, BREAKEND, NO_VARIATION, SYMBOLIC, MIXED  ;
}
