/*
author:  taoliu_alex

time: 2017.3

title :mark and sweep for garbage colloction

*/

#include <stdio.h>
#include <stdlib.h>
#include <glib.h>
#include "gc.h"
#include "../destor.h"
#include "../jcr.h"
#include "../recipe/recipestore.h"
#include "../storage/containerstore.h"
#include "../utils/bloom_filter.h"
#include "../index/fingerprint_cache.h"
#include "../index/kvstore_htable.c"
#include "../restore.h"
//extern void* read_recipe_thread_gc(void *arg);
extern void* read_recipe_thread_gc();
int number_of_bf_fp=0;

static int64_t gc_count=0;

void Destory_rc_list()
{
    struct rc_list *p = gchead;
    while(gchead!=NULL)
    {
         p = gchead;
         free(p);
         gchead = gchead->next;
    }
}


void add_to_rc(struct rc_list* rc_data)
{

    struct rc_list *node,*htemp;

    if (!(node=(struct rc_list *)malloc(sizeof(struct rc_list))))//分配空间
    {
        printf("fail alloc space!\n");  
    }
    else
    {

        node->id=rc_data->id;//保存数据

        //fingerprint *ft = malloc(sizeof(fingerprint));

        memcpy(&node->fp, &rc_data->fp, sizeof(fingerprint));
        
        //node->fp=ft;//保存数据
        
        node->next=NULL;//设置结点指针为空，即为表尾；

        if (gchead==NULL)

        {
            gchead=node;
          
        }
        else
        {
            htemp=gchead;
    
            while(htemp->next!=NULL)
            {
                htemp=htemp->next;
               
            }

            htemp->next=node;
        }

    }
}


void get_delete_message()
{
    int64_t size;
    printf("the  gc_count is %ld\n",gc_count);
    size=gc_count*4/1024;
    printf("garbage collection finished\n");
    printf("the collection  size is %ld MB\n", size);
}



void mark_weep_gc(int jobid)
{

    init_recipe_store();
    bf=(char *)malloc(FILTER_SIZE_BYTES*sizeof(char));
    int j;
    for(j=0;j<FILTER_SIZE_BYTES;j++) {  
        bf[j]=0;  
    }  

    printf("new_backup_version_count is %d\n",new_backup_version_count-1);
    printf("the jobid is%d\n", jobid);

    int i;
    printf("%d %d %d\n", new_backup_version_count, i, jobid);
    for (i = new_backup_version_count-1; i >=0; i--)
    {
        if (i==jobid)
        {
            continue;
        }
        //init_container_store();
        printf("read recipe version is %d \n",i);
        init_gc_jcr(i);
//        restore_recipe_queue = sync_queue_new(100);
        if (jcr.bv->deleted!=1)
        {
 //           pthread_t recipe_t;
 //           pthread_create(&recipe_t, NULL, read_recipe_thread_gc, NULL);
            read_recipe_thread_gc();
            printf("number_of_bf_fp is %d\n",number_of_bf_fp);
        }     
    }

    struct backupVersion* bv = open_backup_version(jobid);
    bv->deleted = 1;
    update_backup_version(bv);
    free_backup_version(bv);

    init_kvstore_htable_gc();

    get_delete_message();

    //close_container_store();
    close_recipe_store();
}


void init_kvstore_htable_gc(){
    kvpair_size = destor.index_key_size + destor.index_value_length * 8;

    if(destor.index_key_size >=4)
        htable = g_hash_table_new_full(g_int_hash, g_feature_equal,
            free_kvpair, NULL);
    else
        htable = g_hash_table_new_full(g_feature_hash, g_feature_equal,
            free_kvpair, NULL);

    sds indexpath = sdsdup(destor.working_directory);
    indexpath = sdscat(indexpath, "index/htable");

    
    FILE *fp;
    if ((fp = fopen(indexpath, "r"))) {
       
        int key_num;
        fread(&key_num, sizeof(int), 1, fp);
        for (; key_num > 0; key_num--) {
           
            kvpair kv = new_kvpair();
            fread(get_key(kv), destor.index_key_size, 1, fp);

            int id_num, i;
            fread(&id_num, sizeof(int), 1, fp);
            assert(id_num <= destor.index_value_length);

            for (i = 0; i < id_num; i++)
            {
                 /* Read an ID */
                fread(&get_value(kv)[i], sizeof(int64_t), 1, fp); 

            }

            if(in_dict(bf,get_key(kv),sizeof(fingerprint))==0)
            {
                struct rc_list *rc_data;
                rc_data=(struct rc_list*)malloc(sizeof(struct rc_list));
                
                //rc_data->fp=get_key(kv);
                memcpy(&rc_data->fp,&get_key(kv),destor.index_key_size);
                //rc_data->id=&get_value(kv);
                rc_data->id= get_value(kv);
                add_to_rc(rc_data);
                gc_count++;
            }

            g_hash_table_insert(htable, get_key(kv), kv);
        }
        fclose(fp);
    }

    sdsfree(indexpath);

   
}



void init_gc_jcr(int revision) {

    init_parameter_gc_jcr();

    jcr.bv = open_backup_version(revision);

    if(jcr.bv->deleted == 1){
        WARNING("The backup has been deleted!");
        exit(1);
    }

    jcr.id = revision;
}


void init_parameter_gc_jcr() {
    jcr.path = NULL;

    jcr.bv = NULL;

    jcr.id = TEMPORARY_ID;

    jcr.status = JCR_STATUS_INIT;

    jcr.file_num = 0;
    jcr.data_size = 0;
    jcr.unique_data_size = 0;
    jcr.chunk_num = 0;
    jcr.unique_chunk_num = 0;
    jcr.zero_chunk_num = 0;
    jcr.zero_chunk_size = 0;
    jcr.rewritten_chunk_num = 0;
    jcr.rewritten_chunk_size = 0;

    jcr.sparse_container_num = 0;
    jcr.inherited_sparse_num = 0;
    jcr.total_container_num = 0;

    jcr.total_time = 0;
    /*
     * the time consuming of seven backup phase
     */
    jcr.read_time = 0;
    jcr.chunk_time = 0;
    jcr.hash_time = 0;
    jcr.dedup_time = 0;
    jcr.rewrite_time = 0;
    jcr.filter_time = 0;
    jcr.write_time = 0;

    /*
     * the time consuming of three restore phase
     */
    jcr.read_recipe_time = 0;
    jcr.read_chunk_time = 0;
    jcr.write_chunk_time = 0;

    jcr.read_container_num = 0;
}
