//
//  main.c
//  443_a2_external_sorting
//
//  Created by HUADONG XING on 2019-02-19.
//  Copyright © 2019 HUADONG XING. All rights reserved.
//

#include <stdio.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <sys/uio.h>
#include <unistd.h>
#include <sys/stat.h>


#define RECORD_SIZE 64
#define DB_SIZE 32000000

// Global counter
int global_page_read;
int global_page_written;

// debug
int debug = 1;

// parameter
int inDB_fd;
long B;
long page_size;
long field;

// useful info
int record_per_page;
int total_page_count;


// struct
struct block {
    int bid;
    char *start_addr;
    int page_count;
    struct block *next;
};

struct in_buffer {
    char *payload;
    int parent_bid;
    int parent_block_page_remain;
    int parent_block_page_count;
    int curr_ptr;
};

struct record {
    char first_name[12];
    char last_name[14];
    char email[38];
} __attribute__ ((packed)) ;

struct sorting_cell {
    struct record *payload;
    int cell_id;
};



// define the compare function
static int compare_record(const void* a, const void* b)
{
    struct record *r1 = (struct record *)a;
    struct record *r2 = (struct record *)b;
    
    if (field == 1) { // compare two record on last name if field = 1.
        return strcmp(r1->last_name, r2->last_name);
    } else {
        printf("Sorting on other field is not yet implemented. Try field = 1.\n");
        exit(-1);
    }
    
    
    // setting up rules for comparison
    return 0;
}

static int compare_cell(const void *a, const void* b){
    struct sorting_cell *c1 = (struct sorting_cell *)a;
    struct sorting_cell *c2 = (struct sorting_cell *)b;
    
    if (field == 1) { // compare two record on last name if field = 1.
        return strcmp(c1->payload->last_name, c2->payload->last_name);
    } else {
        printf("Sorting on other field is not yet implemented. Try field = 1.\n");
        exit(-1);
    }
    
    return 0;
    
}

static void find_min(struct sorting_cell *list, int iter, int compare_funt(const void *, const void*)){
    struct sorting_cell *min = list;
    for (int i = 1; i < iter; i++) {
        if (compare_funt((const void *)min, (const void *)(&list[i])) > 0) {
            min = list + i;
        }
    }
    *list = *min;
}


// for the pass > then 0, do k-way merge (k = B - 1)
// read input from temp_sorted.db and write to temp.db
// after sorting finish, rm temp_sorted.db and rename
// temp.db to temp_sorted.db
int merge(int pass_iter){
    // base on paas_iter number, compute some useful info
    int page_per_block = pow(B - 1, pass_iter - 1); // block size in page
    // check if the last block is full
    int page_in_last_block = page_per_block;
    if (total_page_count%page_per_block) {
        page_in_last_block = total_page_count%page_per_block;
    }
    
    int block_count = total_page_count/page_per_block;
    if (total_page_count%page_per_block) {
        block_count++;
    }
    int inner_iter_count = block_count/(B - 1);
    if (block_count%(B - 1)) {
        inner_iter_count++;
    }
    
    int record_per_page = page_size/RECORD_SIZE;
    
    // handle temp file
    if (rename("output.temp", "input.temp")) {
        perror("rename output.temp to input.temp fail");
        exit(-1);
    }
    
    int input_fd = open("input.temp", O_RDONLY);
    if (input_fd == -1) {
        perror("fail to open input.temp");
        exit(-1);
    }
    
    int output_fd = open("output.temp", O_WRONLY|O_CREAT);
    if (output_fd == -1) {
        perror("open");
        exit(-1);
    }
    if (chmod("output.temp", S_IRUSR) == -1) { // fix permission
        perror("chmod");
        exit(-1);
    };
    

    // inner loop.
    // for each inner iteration
    for (int k = 0; k < inner_iter_count; k++) {
        
        int block_count_in_this_iter = B - 1;
        // check if this is the last iteration
        if (k == inner_iter_count - 1) {
            // if this is the last lteration, check if this can use
            // full buffer
            if (block_count%(B - 1)) {
                // only handle the block that has content
                int remaining_block = block_count%(B - 1);
                
                block_count_in_this_iter = remaining_block;
                
            }
        }
        
        
        // create buffer base on block info
        // we will use only B buffer page to store records
        // B - 1 as input buffer page, 1 as output buffer page
        struct in_buffer *in_buffer = malloc(sizeof(struct in_buffer) * block_count_in_this_iter);
        
        // for block in iteration
        // create proper in_buffer
        for (int i = 0; i < block_count_in_this_iter; i++) {
            // block id
            in_buffer[i].parent_bid = k * (B - 1) + i;
            
            // create and load record to buffer
            in_buffer[i].payload = malloc(page_size);
            pread(input_fd, in_buffer[i].payload, page_size, (in_buffer[i].parent_bid * page_per_block) * page_size);
            global_page_read++;
            in_buffer[i].curr_ptr = 0;
            
            // block remain
            // if this is the last buffer corresponding to the last block
            if ((k == inner_iter_count - 1) && (i == block_count_in_this_iter - 1) ) {
                if (total_page_count%page_per_block) {
                    in_buffer[i].parent_block_page_remain = (total_page_count%page_per_block) - 1;
                    in_buffer[i].parent_block_page_count = total_page_count%page_per_block;
                    break;
                }
            }
            in_buffer[i].parent_block_page_remain = page_per_block - 1;
            in_buffer[i].parent_block_page_count = page_per_block;
        }
        
        
        
        // sorting part
        
        struct record *out_buffer = malloc(page_size);
        int out_buffer_capcity = 0;
        
        // if this is the last iteration
        int page_need2_write = 0;
        if (k == inner_iter_count - 1) {
            page_need2_write = page_per_block * (block_count_in_this_iter - 1);
            page_need2_write += page_in_last_block;
        } else {
            page_need2_write = page_per_block * block_count_in_this_iter;
        }
        
        int written = 0;
        while (!(written == page_need2_write)) {
            
            // for each buffer, pick the element at curr_ptr, add to a new list,
            // use quick sort to sort it, and pick the first record to add to the
            // out_buffer, once out_buffer is full, write it to disk.
            
            
            
            struct sorting_cell *sorting_list = malloc(sizeof(struct sorting_cell) * block_count_in_this_iter);
            // pick record in buffer and put it in sorting list
            for (int i = 0; i < block_count_in_this_iter; i++) {
                sorting_list[i].cell_id = i;
                sorting_list[i].payload = in_buffer[i].payload + (in_buffer[i].curr_ptr * RECORD_SIZE);
            }
            
//            mergesort(sorting_list, block_count_in_this_iter, sizeof(struct sorting_cell), compare_cell);
            find_min(sorting_list, block_count_in_this_iter, compare_cell);
            
            // pick the first element in sorting_list, add it to output buffer
            // move the corresponding in_buffer's ptr to next
            memcpy(out_buffer+out_buffer_capcity, sorting_list[0].payload, RECORD_SIZE);
            out_buffer_capcity++;
            
            // check if out_buffer is full, if full write it to disk
            if (out_buffer_capcity == record_per_page) {
                out_buffer_capcity = 0;
                pwrite(output_fd, out_buffer, page_size, (k * (B - 1) * page_per_block + written)*page_size);
                written++;
                global_page_written++;
            }
            
            in_buffer[sorting_list[0].cell_id].curr_ptr++;
            // if curr_ptr larger then record per page
            // load the next page in the corresponding block
            
            if (in_buffer[sorting_list[0].cell_id].curr_ptr == record_per_page) { //
                // check if there is still none read page in the block
                if (in_buffer[sorting_list[0].cell_id].parent_block_page_remain > 0) {
                    pread(input_fd, in_buffer[sorting_list[0].cell_id].payload, page_size, (((in_buffer[sorting_list[0].cell_id].parent_bid * page_per_block) + (in_buffer[sorting_list[0].cell_id].parent_block_page_count - in_buffer[sorting_list[0].cell_id].parent_block_page_remain)) * page_size));
                    in_buffer[sorting_list[0].cell_id].parent_block_page_remain--;
                    global_page_read++;
                } else { // all the page in the block is read
                    memset(in_buffer[sorting_list[0].cell_id].payload, 0xFF, page_size);
                }
                
                in_buffer[sorting_list[0].cell_id].curr_ptr = 0;
            }
            
            free(sorting_list);
        }
        
        
        
        // free all buffer
        for (int i = 0; i < block_count_in_this_iter; i++) {
            free(in_buffer[i].payload);
        }
        free(in_buffer);
        free(out_buffer);
    }
    
    // remove the input file
    close(input_fd);
    close(output_fd);
    if (remove("input.temp")) {
        perror("fail to remove input.temop");
        exit(-1);
    }
    
    return output_fd;
    
    
}

// for the pass 0, we will sort each record in a single page
// and write it to init_pass.db
// return the fd of init_pass.db
int init_pass(){
    // if there is an output.temp delete it
    if (remove("output.temp")) {
        //printf("there is no output.temp, work space clear\n");
    }
    // create the temp file
    int init_pass_fd = open("output.temp", O_WRONLY|O_CREAT);
    if (init_pass_fd == -1) {
        perror("open");
        exit(-1);
    }
    if (chmod("output.temp", S_IRUSR) == -1) {
        perror("chmod");
        exit(-1);
    };
    
    char *buffer = malloc(page_size);
    // for every page, read it and sort it one by one
    for (int i = 0; i < total_page_count; i++) {
        // read page in memory
        if (pread(inDB_fd, buffer, page_size, i * page_size) != page_size) {
            perror("init_pass: pread");
            exit(-1);
        }
        global_page_read++;
        // sorting
        qsort(buffer, record_per_page, RECORD_SIZE, compare_record);
        
        // write sorted page to file
        pwrite(init_pass_fd, buffer, page_size, i * page_size);
        global_page_written++;
    }
    free(buffer);
    close(init_pass_fd);
    return init_pass_fd;
}

int main(int argc, const char * argv[]) {
    if (argc != 6) {
        fprintf(stderr, "Usage: <inDB> <outDb> <B> <pSize> <field>\n");
        return -1;
    }
    
    inDB_fd = open(argv[1], O_RDONLY);
    B = strtol(argv[3], NULL, 10);
    page_size = strtol(argv[4], NULL, 10);
    field = strtol(argv[5], NULL, 10);
    
    global_page_written = 0;
    global_page_read = 0;
    
    // compute some useful info
    record_per_page = page_size / RECORD_SIZE;
    total_page_count = DB_SIZE/page_size;
    
    double iteration = log(32000000/page_size)/log(B - 1);
    int iteration2 = ceil(iteration);
    
    // for pass 0
    init_pass();
    
    // merge
    int iter;
    for (iter = 1; iter < iteration2 + 1; iter++) {
//        printf("pass:%d\n", iter);
        merge(iter);
    }
    
    if (rename("output.temp", argv[2])) {
        perror("rename output.temp to outDB fail");
        exit(-1);
    }
    
    // print result
    printf("pass count: %d\n", iter - 1);
    printf("page read: %d\n", global_page_read);
    printf("page written: %d\n", global_page_written);
    
    return 0;
}
