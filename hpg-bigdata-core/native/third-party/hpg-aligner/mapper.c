#include <stdio.h>
#include <stdlib.h>

//----------------------------------------------------------------------------------------------------------------------
//
//----------------------------------------------------------------------------------------------------------------------

char *map(char *fastq, char *index_path) {
    char *sam = (char *) calloc(100, sizeof(char));
    sprintf(sam, "read1\t10\t20\t100M\tATAAATTACGGGGGAGA\nread2\t10\t20\t100M\tATAAATTACGGGGGAGA\n");
    //printf("Mapping...\n%s\n%s\n...done!\n", fastq, sam);
    return sam;
}
