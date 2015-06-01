#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "aligners/bwt/bwt.h"

//----------------------------------------------------------------------------------------------------------------------
//
//----------------------------------------------------------------------------------------------------------------------

void *load_index(const char *index_path) {

    printf("Loading index...\n");
    bwt_index_t *index = bwt_index_new(index_path, false);
    printf("...done! (%x)\n", index);

    //char *index = (char *) calloc(100, sizeof(char));
    //sprintf(index, "index located at %s", index_path);
    //printf("Loading index...\n\t%s\n...done!\n", index);
    return (void *) index;
}

//----------------------------------------------------------------------------------------------------------------------
//
//----------------------------------------------------------------------------------------------------------------------

void free_index(void *index) {
    if (index) {
        printf("Freeing index (%x)...\n", (bwt_index_t *) index);
        bwt_index_free((bwt_index_t *) index);
        printf("...done!\n");

        //free((char *) index);
    }
}

//--------------------------------------------------------------------------------
//--------------------------------------------------------------------------------
