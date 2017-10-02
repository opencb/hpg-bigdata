/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package org.opencb.hpg.bigdata.app.cli.local.avro;
@SuppressWarnings("all")
public class ConsequenceType {
   private String geneName;
   private String ensemblGeneId;
   private String ensemblTranscriptId;
   private String strand;
   private String biotype;
   private java.util.List<ExonOverlap> exonOverlap;
   private java.util.List<String> transcriptAnnotationFlags;
   private Integer cdnaPosition;
   private Integer cdsPosition;
   private String codon;
   private ProteinVariantAnnotation proteinVariantAnnotation;
   private java.util.List<SequenceOntologyTerm> sequenceOntologyTerms;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public ConsequenceType() {}

  /**
   * All-args constructor.
   */
  public ConsequenceType(String geneName, String ensemblGeneId, String ensemblTranscriptId, String strand, String biotype, java.util.List<ExonOverlap> exonOverlap, java.util.List<String> transcriptAnnotationFlags, Integer cdnaPosition, Integer cdsPosition, String codon, ProteinVariantAnnotation proteinVariantAnnotation, java.util.List<SequenceOntologyTerm> sequenceOntologyTerms) {
    this.geneName = geneName;
    this.ensemblGeneId = ensemblGeneId;
    this.ensemblTranscriptId = ensemblTranscriptId;
    this.strand = strand;
    this.biotype = biotype;
    this.exonOverlap = exonOverlap;
    this.transcriptAnnotationFlags = transcriptAnnotationFlags;
    this.cdnaPosition = cdnaPosition;
    this.cdsPosition = cdsPosition;
    this.codon = codon;
    this.proteinVariantAnnotation = proteinVariantAnnotation;
    this.sequenceOntologyTerms = sequenceOntologyTerms;
  }

  /**
   * Gets the value of the 'geneName' field.
   */
  public String getGeneName() {
    return geneName;
  }

  /**
   * Sets the value of the 'geneName' field.
   * @param value the value to set.
   */
  public void setGeneName(String value) {
    this.geneName = value;
  }

  /**
   * Gets the value of the 'ensemblGeneId' field.
   */
  public String getEnsemblGeneId() {
    return ensemblGeneId;
  }

  /**
   * Sets the value of the 'ensemblGeneId' field.
   * @param value the value to set.
   */
  public void setEnsemblGeneId(String value) {
    this.ensemblGeneId = value;
  }

  /**
   * Gets the value of the 'ensemblTranscriptId' field.
   */
  public String getEnsemblTranscriptId() {
    return ensemblTranscriptId;
  }

  /**
   * Sets the value of the 'ensemblTranscriptId' field.
   * @param value the value to set.
   */
  public void setEnsemblTranscriptId(String value) {
    this.ensemblTranscriptId = value;
  }

  /**
   * Gets the value of the 'strand' field.
   */
  public String getStrand() {
    return strand;
  }

  /**
   * Sets the value of the 'strand' field.
   * @param value the value to set.
   */
  public void setStrand(String value) {
    this.strand = value;
  }

  /**
   * Gets the value of the 'biotype' field.
   */
  public String getBiotype() {
    return biotype;
  }

  /**
   * Sets the value of the 'biotype' field.
   * @param value the value to set.
   */
  public void setBiotype(String value) {
    this.biotype = value;
  }

  /**
   * Gets the value of the 'exonOverlap' field.
   */
  public java.util.List<ExonOverlap> getExonOverlap() {
    return exonOverlap;
  }

  /**
   * Sets the value of the 'exonOverlap' field.
   * @param value the value to set.
   */
  public void setExonOverlap(java.util.List<ExonOverlap> value) {
    this.exonOverlap = value;
  }

  /**
   * Gets the value of the 'transcriptAnnotationFlags' field.
   */
  public java.util.List<String> getTranscriptAnnotationFlags() {
    return transcriptAnnotationFlags;
  }

  /**
   * Sets the value of the 'transcriptAnnotationFlags' field.
   * @param value the value to set.
   */
  public void setTranscriptAnnotationFlags(java.util.List<String> value) {
    this.transcriptAnnotationFlags = value;
  }

  /**
   * Gets the value of the 'cdnaPosition' field.
   */
  public Integer getCdnaPosition() {
    return cdnaPosition;
  }

  /**
   * Sets the value of the 'cdnaPosition' field.
   * @param value the value to set.
   */
  public void setCdnaPosition(Integer value) {
    this.cdnaPosition = value;
  }

  /**
   * Gets the value of the 'cdsPosition' field.
   */
  public Integer getCdsPosition() {
    return cdsPosition;
  }

  /**
   * Sets the value of the 'cdsPosition' field.
   * @param value the value to set.
   */
  public void setCdsPosition(Integer value) {
    this.cdsPosition = value;
  }

  /**
   * Gets the value of the 'codon' field.
   */
  public String getCodon() {
    return codon;
  }

  /**
   * Sets the value of the 'codon' field.
   * @param value the value to set.
   */
  public void setCodon(String value) {
    this.codon = value;
  }

  /**
   * Gets the value of the 'proteinVariantAnnotation' field.
   */
  public ProteinVariantAnnotation getProteinVariantAnnotation() {
    return proteinVariantAnnotation;
  }

  /**
   * Sets the value of the 'proteinVariantAnnotation' field.
   * @param value the value to set.
   */
  public void setProteinVariantAnnotation(ProteinVariantAnnotation value) {
    this.proteinVariantAnnotation = value;
  }

  /**
   * Gets the value of the 'sequenceOntologyTerms' field.
   */
  public java.util.List<SequenceOntologyTerm> getSequenceOntologyTerms() {
    return sequenceOntologyTerms;
  }

  /**
   * Sets the value of the 'sequenceOntologyTerms' field.
   * @param value the value to set.
   */
  public void setSequenceOntologyTerms(java.util.List<SequenceOntologyTerm> value) {
    this.sequenceOntologyTerms = value;
  }

}
