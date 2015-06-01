#include "org_opencb_hpg_bigdata_core_NativeAligner.h"
#include <stdlib.h>
#include <stdio.h>
#include <malloc.h>

#include "hpg-aligner.h"

//------------------------------------------------------------------------------//

JNIEXPORT jlong JNICALL Java_org_opencb_hpg_bigdata_core_NativeAligner_load_1params
  (JNIEnv *env, jobject this, jobject params_obj) {

    aligner_params_t *params = aligner_params_new();

    // get the class of the object parameter
	jclass cls = (*env)->GetObjectClass(env, params_obj);

	if (cls) {

	    jfieldID fid_int, fid_float, fid_string;
	    jstring val_string;
	    char *str;

        // get the field ID of the "numSeeds" field of this class
	    fid_int = (*env)->GetFieldID(env, cls, "numSeeds", "I");
	    if (fid_int) {
    	    params->num_seeds = (*env)->GetIntField(env, params_obj, fid_int);
    	} else {
    	    printf("Error getting num_seeds parameter\n");
    	}

        // get the field ID of the "minSWScore" field of this class
	    fid_float = (*env)->GetFieldID(env, cls, "minSWScore", "F");
	    if (fid_float) {
    	    params->min_sw_score = (*env)->GetFloatField(env, params_obj, fid_float);
    	} else {
    	    printf("Error getting minSWScore parameter\n");
    	}

        // get the field ID of the "seqFileName1" field of this class
	    fid_string = (*env)->GetFieldID(env, cls, "seqFileName1", "Ljava/lang/String;");
	    if (fid_string) {
	        val_string = (jstring) (*env)->GetObjectField(env, params_obj, fid_string);
	        if (val_string) {
                str = (*env)->GetStringUTFChars(env, val_string, NULL);
                params->seq_filename1 = strdup(str);
                (*env)->ReleaseStringUTFChars(env, val_string, str);
	        } else {
                printf("Error getting String object (seqFileName1)\n");
	        }
    	} else {
    	    printf("Error getting seqFileName1 parameter\n");
    	}

        // get the field ID of the "seqFileName2" field of this class
	    fid_string = (*env)->GetFieldID(env, cls, "seqFileName2", "Ljava/lang/String;");
	    if (fid_string) {
	        val_string = (jstring) (*env)->GetObjectField(env, params_obj, fid_string);
	        if (val_string) {
                str = (*env)->GetStringUTFChars(env, val_string, NULL);
                params->seq_filename2 = strdup(str);
                (*env)->ReleaseStringUTFChars(env, val_string, str);
	        } else {
                printf("Error getting String object (seqFileName2)\n");
	        }
    	} else {
    	    printf("Error getting seqFileName2 parameter\n");
    	}

        // get the field ID of the "resultFileName" field of this class
	    fid_string = (*env)->GetFieldID(env, cls, "resultFileName", "Ljava/lang/String;");
	    if (fid_string) {
	        val_string = (jstring) (*env)->GetObjectField(env, params_obj, fid_string);
	        if (val_string) {
                str = (*env)->GetStringUTFChars(env, val_string, NULL);
                params->out_dirname = strdup(str);
                (*env)->ReleaseStringUTFChars(env, val_string, str);
	        } else {
                printf("Error getting String object (resultFileName)\n");
	        }
    	} else {
    	    printf("Error getting resultFileName parameter\n");
    	}

        // get the field ID of the "indexFolderName" field of this class
	    fid_string = (*env)->GetFieldID(env, cls, "indexFolderName", "Ljava/lang/String;");
	    if (fid_string) {
	        val_string = (jstring) (*env)->GetObjectField(env, params_obj, fid_string);
	        if (val_string) {
                str = (*env)->GetStringUTFChars(env, val_string, NULL);
                params->index_dirname = strdup(str);
                (*env)->ReleaseStringUTFChars(env, val_string, str);
	        } else {
                printf("Error getting String object (indexFolderName)\n");
	        }
    	} else {
    	    printf("Error getting indexFolderName parameter\n");
    	}

    } else {
        printf("Error getting Java class\n");
    }

    printf("Java_org_opencb_hpg_bigdata_core_NativeAligner_load_1params, params:\n");
    aligner_params_display((aligner_params_t *) params);

    return ((long) params);
}

//------------------------------------------------------------------------------//

JNIEXPORT void JNICALL Java_org_opencb_hpg_bigdata_core_NativeAligner_free_1params
  (JNIEnv *env, jobject this, jlong params) {

  aligner_params_free((aligner_params_t *) params);
}


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
  (JNIEnv *env, jobject this, jstring fastq, jlong index, jlong params) {

    const char *reads = (*env)->GetStringUTFChars(env, fastq, NULL);
    char *sam = map(reads, (void *)index);

    printf("Java_org_opencb_hpg_bigdata_core_NativeAligner_map, params:\n");
    aligner_params_display((aligner_params_t *) params);

    jstring res = (*env)->NewStringUTF(env, sam);

    // free memory
    if (sam) free(sam);

    // return
	return res;
}

//------------------------------------------------------------------------------//
//------------------------------------------------------------------------------//
