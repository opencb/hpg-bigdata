//--------------------------------------------------------------------------------
//
//--------------------------------------------------------------------------------

typedef struct aligner_params {
    int num_seeds;
    float min_sw_score;

    char *seq_filename1;
    char *seq_filename2;
    char *out_dirname;
    char *index_dirname;

} aligner_params_t;

inline void aligner_params_display(aligner_params_t *params) {
    if (params) {
        printf("num. seeds: %i\n", params->num_seeds);
        printf("min. SW score: %0.2f\n", params->min_sw_score);

        printf("\n");
        printf("seq. filename #1: %s\n", params->seq_filename1);
        printf("seq. filename #2: %s\n", params->seq_filename2);
        printf("\n");
        printf("out dir. name  : %s\n", params->out_dirname);
        printf("index dir. name: %s\n", params->index_dirname);

    }
}

inline aligner_params_t *aligner_params_new() {
    aligner_params_t *res = (aligner_params_t *) calloc(1, sizeof(aligner_params_t));
    return res;
}

inline void aligner_params_free(aligner_params_t *params) {
    if (params) {
        printf("freeing aligner params:\n");
        aligner_params_display(params);

        if (params->seq_filename1) free(params->seq_filename1);
        if (params->seq_filename2) free(params->seq_filename2);
        if (params->out_dirname) free(params->out_dirname);
        if (params->index_dirname) free(params->index_dirname);

        free(params);
    }
}

//--------------------------------------------------------------------------------

void *load_index(const char *index_path);
void free_index(void *index);

char *map(const char *fastq, void *index);

//--------------------------------------------------------------------------------
//--------------------------------------------------------------------------------
