#include "hpg-aligner.h"

//----------------------------------------------------------------------------------------------------------------------
// main
//----------------------------------------------------------------------------------------------------------------------

void main() {
    // load index
    char *index_path = "/toto/index.sa";
    void *index = load_index(index_path);

    // mapping
    char *fastq = "@read1\nACTACTACTACTGG\n+\n22222222222222\n@read2\nGAGTTCCAAAGGGG\n+\n22222222222222\n";
    char *sam = map(fastq, index);

    printf("fastq:\n%s\n", fastq);
    printf("sam:\n%s\n", sam);

    // free memory
    free(sam);
    free_index(index);
}

//--------------------------------------------------------------------------------
//--------------------------------------------------------------------------------
