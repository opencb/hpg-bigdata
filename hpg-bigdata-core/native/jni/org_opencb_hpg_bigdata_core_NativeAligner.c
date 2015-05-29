#include "org_opencb_hpg_bigdata_core_NativeAligner.h"
#include <stdlib.h>
#include <stdio.h>
#include <malloc.h>

//------------------------------------------------------------------------------//

void *load_index(const char *index_path);
void free_index(void *index);

char *map(const char *fastq, void *index);

//------------------------------------------------------------------------------//

JNIEXPORT jlong JNICALL Java_org_opencb_hpg_bigdata_core_NativeAligner_load_1index
  (JNIEnv *env, jobject this, jstring index_path) {

  const char *path = (*env)->GetStringUTFChars(env, index_path, NULL);
  void *index = load_index(path);
  return ((long) index);
}

//------------------------------------------------------------------------------//

JNIEXPORT void JNICALL Java_org_opencb_hpg_bigdata_core_NativeAligner_free_1index
  (JNIEnv *env, jobject this, jlong index) {

  free_index((void *) index);
}

//------------------------------------------------------------------------------//

JNIEXPORT jstring JNICALL Java_org_opencb_hpg_bigdata_core_NativeAligner_map
  (JNIEnv *env, jobject this, jstring fastq, jlong index) {

    const char *reads = (*env)->GetStringUTFChars(env, fastq, NULL);
    char *sam = map(reads, (void *)index);

    jstring res = (*env)->NewStringUTF(env, sam);

    // free memory
    if (sam) free(sam);

    // return
	return res;
}

//------------------------------------------------------------------------------//
//------------------------------------------------------------------------------//
