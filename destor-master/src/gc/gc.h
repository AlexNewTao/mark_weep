

#ifndef GC_H
#define GC_H

#include "../utils/bloom_filter.h"

typedef unsigned char fingerprint[20];
typedef int64_t containerid;

unsigned char *bf;

int64_t number_of_bf_fp;

struct rc_list{
    containerid id;
    fingerprint fp;
    struct rc_list *next;
};

struct rc_list *gchead;

void Destory_rc_list();

void add_to_rc(struct rc_list* rc_data);


void get_delete_message();

void init_parameter_gc_jcr();

void init_gc_jcr(int revision);

#endif
