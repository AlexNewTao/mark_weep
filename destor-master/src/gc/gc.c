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
extern void* read_recipe_thread_del();


int64_t number_of_bf_fp=0;

static int64_t gc_count=0;

int delete_number;

int del_arr[del_arr_size];

static FILE* fp;

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

void init_bloom_filter()
{
    bf=(char *)malloc(FILTER_SIZE_BYTES*sizeof(char));
    int j;
    for(j=0;j<FILTER_SIZE_BYTES;j++) {  
        bf[j]=0;  
    } 

}

void init_delete_bloom_filter()
{
    del_bf=(char *)malloc(FILTER_SIZE_BYTES*sizeof(char));
    int j;
    for(j=0;j<FILTER_SIZE_BYTES;j++) {  
        del_bf[j]=0;  
    } 

}


void mark_weep_gc(int jobid)
{

    init_recipe_store();

    init_bloom_filter();

    TIMER_DECLARE(1);
    TIMER_BEGIN(1);

    //init_delete_bloom_filter();
    number_of_bf_fp=0;

    get_del_arr_message();

    //show_del_arr();

    if (contain_in_del_arr(jobid))
    {
    	printf("the %d has been delete !\n", jobid);
    }
    else
    {
    	/*int i;
    	printf("%d %d %d\n", new_backup_version_count, i, jobid);
	    for (i = new_backup_version_count-1; i >=0; i--)
	    {
	        if (i==jobid)
	        {
	            continue;
	        }
	        printf("read recipe version is %d \n",i);
	        init_gc_jcr(i);
	        printf("jcr.bv num is %d\n",jcr.bv->bv_num);
	        printf("jcr.bv jcr.bv->number_of_files  is %d\n",jcr.bv->number_of_files);

	        read_recipe_thread_gc();
	        printf("number_of_bf_fp is %ld\n",number_of_bf_fp);   
	    }*/
        gc_read_recipe(jobid);
	    delete_number=delete_number+1;
	    int j;
	    for (j = 0; j < del_arr_size; j++)
	    {
	    	if (j==delete_number-1)
	    	{
	    		del_arr[j]=jobid;
	    	}
	    }

	    printf("after delete_number is %d\n",delete_number );
	    //printf("after the del_arr as follow!\n");
	    //show_del_arr();
	    //write_deleteversion_to_disk();
	    init_kvstore_htable_gc();
    }
    
    get_delete_message();

    

    Destory_rc_list();

    close_recipe_store();

    TIMER_END(1, jcr.gc_total_time);
    
    char logfile[] = "gc.log";
    FILE *fp = fopen(logfile, "a");

    fprintf(fp, " %d %.4f %.4f %.4f %ld \n",
        jobid,
        jcr.gc_total_time / 1000000,
        jcr.gc_read_recipe_time / 1000000,
        jcr.gc_creat_gclist_time / 1000000,
        gc_count*4/1024);

            
    fclose(fp);

}

void gc_read_recipe(int n)
{

    TIMER_DECLARE(1);
    TIMER_BEGIN(1);

    int i;
    printf("%d %d %d\n", new_backup_version_count, i, n);
    for (i = new_backup_version_count-1; i >=0; i--)
    {
        if (i==n)
        {
            continue;
        }
        printf("read recipe version is %d \n",i);
        init_gc_jcr(i);
        printf("jcr.bv num is %d\n",jcr.bv->bv_num);
        printf("jcr.bv jcr.bv->number_of_files  is %d\n",jcr.bv->number_of_files);

        read_recipe_thread_gc();
        printf("number_of_bf_fp is %ld\n",number_of_bf_fp);   
    }
    TIMER_END(1, jcr.gc_read_recipe_time);
}




int contain_in_del_arr(int n)
{
	int i;
	for (i = 0; i < del_arr_size; i++)
	{
		if (del_arr[i]==n)
		{
			return 1;
		}
	}
	return 0;
}


void get_del_arr_message()
{
	sds dvpath = sdsdup(destor.working_directory);
    dvpath = sdscat(dvpath, "/index/delete_version.dv");

    printf(" access(dvpath,F_OK) %d\n", access(dvpath,F_OK));

    if (access(dvpath,F_OK)!=0)
    {
    	delete_number=0;
    	init_del_arr();
    }
    else //if (access(dvpath,F_OK)==0)
    {
    	//read_del_arr_from_disk();
    	if((fp = fopen(dvpath, "r")))
    	{
    		fread(&delete_number, sizeof(int32_t),1,fp);
	    	fread(&del_arr, sizeof(del_arr),1,fp);
	    	fclose(fp);
    	}
    	else
    	{
    		printf("can't open file\n");
    	}	
    }
    sdsfree(dvpath); 
    printf("before delete_number is %d\n",delete_number);
    printf("before the del_arr as follow!\n");
    //show_del_arr();
}

/*void get_del_arr_message2()
{
	sds dvpath = sdsdup(destor.working_directory);
    dvpath = sdscat(dvpath, "/delete_version.dv");
    int* del_arr=(int32_t *)malloc(sizeof(int32_t)*del_arr_size);

    printf(" access(dvpath,F_OK) %d\n", access(dvpath,F_OK));

    if (access(dvpath,F_OK)!=0)
    {
    	//delete_number=0;
    	init_del_arr2();
    }
    else if (access(dvpath,F_OK)==0)
    {

    	if((fp = fopen(dvpath, "r+")))
    	{
    		fseek(fp,sizeof(int32_t)*del_arr_size,SEEK_SET);
	    	fread(del_arr, sizeof(int32_t),del_arr_size,fp);
	    	fclose(fp);
    	}
    	else
    	{
    		printf("can't open file\n");
    	}	
    }
    sdsfree(dvpath); 
    printf("before the del_arr as follow!\n");
    show_del_arr();
}*/

/*void read_del_arr_from_disk()
{
	sds dvpath = sdsdup(destor.working_directory);
    dvpath = sdscat(dvpath, "/delete_version.dv");
    int* delete_number=(int32_t *)malloc(sizeof(int32_t));
    int* del_arr=(int32_t *)malloc(sizeof(int32_t)*del_arr_size);

	if((fp = fopen(dvpath, "r+")))
	{
		fread(&delete_number, sizeof(int32_t),1,fp);
    	fread(&del_arr, sizeof(int32_t),del_arr_size,fp);
    	fclose(fp);
	}
	sdsfree(dvpath); 
}*/



void init_del_arr()
{
	//int* del_arr=(int32_t *)malloc(sizeof(int32_t)*del_arr_size);
	int i;
	for (i = 0; i < del_arr_size; i++)
	{
		del_arr[i]=init_delete_parameter;
	}
}

/*void init_del_arr2()
{
	//int* del_arr=(int32_t *)malloc(sizeof(int32_t)*del_arr_size);
	del_arr[0]=0;
	int i;
	for (i = 1; i < del_arr_size; i++)
	{
		del_arr[i]=init_delete_parameter;
	}
}*/


void show_del_arr()
{
    int i;
    for (i = 0; i < del_arr_size; i++)
    {
        printf("the delete array is %d\n",del_arr[i]);
    }
}

/*void  read_delete_version()
{
    sds dvpath = sdsdup(destor.working_directory);
    dvpath = sdscat(dvpath, "/delete_version.dv");
    printf("111\n");
    int i=0;
    if((fp = fopen(dvpath, "r")))
    {
        printf("333\n");
    	while(!feof(fp)&&i<delete_array_size)
    	{
    		fread(&d_array[i], sizeof(int32_t),1,fp);
    		i++;
    	}
        printf("444\n");
        fclose(fp);
    }
    sdsfree(dvpath); 
    printf("222\n");

    printf("read delete version successfully\n");

}*/

void write_deleteversion_to_disk() 
{
    sds dvpath = sdsdup(destor.working_directory);
    dvpath = sdscat(dvpath, "/index/delete_version.dv");

    FILE *fp;
    if((fp = fopen(dvpath, "w"))) 
    {
        fwrite(&delete_number, sizeof(int32_t), 1, fp);
        fwrite(&del_arr, sizeof(del_arr),1,fp);
        fclose(fp);
    }
    sdsfree(dvpath); 
    printf("write the del_arr to disk successfully\n");
}

/*void write_deleteversion_to_disk2() 
{
    sds dvpath = sdsdup(destor.working_directory);
    dvpath = sdscat(dvpath, "/delete_version.dv");

    //FILE *fp;
    if((fp = fopen(dvpath, "w+"))) 
    {
        fwrite(&del_arr, sizeof(int32_t),del_arr_size,fp);
        fclose(fp);
    }
    sdsfree(dvpath); 
}
*/

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


    jcr.gc_total_time=0;
    jcr.gc_read_recipe_time=0;
    jcr.gc_creat_gclist_time=0;

    jcr.read_container_num = 0;
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

    TIMER_DECLARE(1);
    TIMER_BEGIN(1);

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
                //printf("%d\n", gc_count);
            }

            g_hash_table_insert(htable, get_key(kv), kv);
        }
        fclose(fp);
    }

    TIMER_END(1, jcr.gc_creat_gclist_time);

    sdsfree(indexpath);

   
}