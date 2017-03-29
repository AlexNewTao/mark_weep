

#ifndef GC_H
#define GC_H

#include "../utils/bloom_filter.h"

#define del_arr_size 10

#define init_delete_parameter -1


typedef unsigned char fingerprint[20];
typedef int64_t containerid;

unsigned char *bf;
unsigned char *del_bf;
int64_t number_of_bf_fp;

struct rc_list{
    containerid id;
    fingerprint fp;
    struct rc_list *next;
};



struct rc_list *gchead;

void Destory_rc_list();

void add_to_rc(struct rc_list* rc_data);

void gc_read_recipe();

void get_delete_message();

void init_parameter_gc_jcr();


void get_del_arr_message();

void init_gc_jcr(int revision);

void init_del_arr();

void show_del_arr();

void write_deleteversion_to_disk();


#endif
