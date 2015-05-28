#include "org_opencb_hpg_bigdata_core_NativeConverter.h"

//------------------------------------------------------------------------------//

void bam2ga(const char *bam_filename, const char *avro_filename, const char *codec_name);

JNIEXPORT void JNICALL Java_org_opencb_hpg_bigdata_core_NativeConverter_bam2ga(JNIEnv *env, jobject this,
																			   jstring bamFilename, jstring gaFilename,
																			   jstring compression) {

  const char *bam_filename = (*env)->GetStringUTFChars(env, bamFilename, NULL);
  const char *ga_filename = (*env)->GetStringUTFChars(env, gaFilename, NULL);
  const char *codec_name = (*env)->GetStringUTFChars(env, compression, NULL);

  printf("converting %s to %s using compression %s\n", bam_filename, ga_filename, codec_name);

  // converting
  bam2ga(bam_filename, ga_filename, codec_name);
}

//------------------------------------------------------------------------------//
//------------------------------------------------------------------------------//
