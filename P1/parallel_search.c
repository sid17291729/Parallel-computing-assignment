#include<mpi.h>
#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<sys/types.h>
#include<sys/stat.h>
#include<fcntl.h>
#include<ctype.h>
#include<unistd.h>
#define MAXLEN 100
//echo 0 | sudo tee /proc/sys/kernel/yama/ptrace_scope

int eof;

/*** Utility functions***/
int get_file_size(int fd)
{
    struct stat buf;
    fstat(fd,&buf);
    
    return buf.st_size;
}
void set_pointer(int fd,int offset)
{
    lseek(fd,offset,SEEK_SET);
}
//correcting the pointer to point to the begining of the word initially.This is done because the region for a process might start in the middle of a particular word.
void correct(int fd,int *line,int *count,int rank)
{
    lseek(fd,-1,SEEK_CUR);
    char c;
    read(fd,&c,1);
    
    
    if(c==32||c==10||c==13)
    {   
        while(c==' '||c=='\n'||c==13)
       {   
            
           int x=read(fd,&c,1);
           if(x==0)
           return;
           (*count)=(*count)+1;

        
        }
        lseek(fd,-1,SEEK_CUR);
        (*count)=(*count)-1;
        
        return;
    }
    int x=read(fd,&c,1);
    (*count)=(*count)+1;
    if(x==-1)
    return;
    while(c!=' '&&c!='\n')
    {    
        x=read(fd,&c,1);
        (*count)=(*count)+1;
        
        if(x==-1)
        return;
    }
    
}

//Linked list Node
typedef struct node{
    char *word;
    int win;
    int lno;
    int wlen;
    struct node *next;
}NODE;
// reading one word at a time from the file
int get_word(int fd,char *buf,int *wlen,int *line)
{
    char c;
    int x=read(fd,&c,1);
    if(x==0)
     {eof=1;return -1;}
     int count=1;
     int ptr=0;
     buf[ptr++]=c;
     *wlen=*wlen+1;
     while(c!=' '&&c!='\n')
     {
       x=read(fd,&c,1);
      
       if(x==0)
       { eof=1;
        return -1*count;}
       count=count+1;
       if(isalnum(c))
         {buf[ptr++]=c;
         *wlen=*wlen+1;}

     }
     if(c=='\n')
     (*line)=(*line)+1;
     while(!(isalnum(c)))
     {  x=read(fd,&c,1);
        if(x==0)
        {   eof=1;
            return -1*count;
        }
        if(c=='\n')
        (*line)=(*line)+1;

        ++count;
     }
     --count;
     lseek(fd,-1,SEEK_CUR);
     return count;
}
//This data structure is used to store the positions of the query word found in the file
typedef struct position
{
 int wno;
 int ind;
 int cap;
 int *ln;
 int *win;   
}POS;

void pos_init(POS * p,int wno)
{
 p->cap=10;
 p->ind=0;
 p->ln=(int *)malloc(sizeof(int)*10);
 p->win=(int *)malloc(sizeof(int)*10);
 p->wno=wno;

}
void add_word(POS *p,int win,int ln,int wno)
{
    p->win[p->ind]=win;
    p->wno=wno;
    p->ln[p->ind]=ln;
    p->ind=p->ind+1;
    if(p->ind==p->cap)
    {   p->cap=p->cap*2;
        p->ln=(int *)realloc(p->ln,sizeof(int)*p->cap);
        p->win=(int *)realloc(p->win,sizeof(int)*p->cap);
    }
}
void pl(NODE *head,int rank)
{
    NODE *t;
    t=head;
    while(t!=NULL)
    {
        printf("rank=%d, word=%s win=%d line=%d\n",rank,t->word,t->win,t->lno);
        t=t->next;
    }
}
int check(NODE *head,char *q_word[],int Q_size)
{
    NODE *t=head;
    for(int i=0;i<Q_size;++i)
    {
        if(strcmp(q_word[i],t->word)!=0)
            return 0;
        t=t->next;
    }
    return 1;
    
}
/***main function***/
int main(int argc, char*argv[] )
{
    int rank,num_tasks;
    MPI_Status Stat;
    //setting up processes
    MPI_Init(&argc,&argv);
    MPI_Comm_size(MPI_COMM_WORLD,&num_tasks);
    MPI_Comm_rank(MPI_COMM_WORLD,&rank);
    
    //opening file and adjusting the pointers for each process
    int fd=open(argv[1],O_RDONLY);
    int file_size=get_file_size(fd);
    int local_size=file_size/num_tasks;
    set_pointer(fd,local_size*rank);
    
    //initializing
    int line=0;
    int count=0;
    if(rank!=0)
    correct(fd,&line,&count,rank);
    if(rank!=num_tasks-1)
    eof=1;
    int word_index=1;
    int AND=0;
    
    //checking whether it is an AND query or a OR query
    if(strcmp(argv[2],"OR")==0)
    AND=0;
    else
    AND=1;

    int Q_size=argc-3;//size of the query
    int qsz;
    if(AND)
    qsz=1;
    else
    qsz=Q_size;

    POS q[qsz];//vector to hold the results
    char *q_word[Q_size];//vector to hold the query
    if(AND)
    pos_init(&q[0],0);
    
    //populating the query vector
    for(int i=0;i<Q_size;++i)
    {  if(!AND)
        pos_init(&q[i],i);
      q_word[i]=(char*)malloc(sizeof(char)*MAXLEN);
      
     strcpy(q_word[i],argv[i+3]);
    }
    
    char buf[MAXLEN];//temporary buffer
    double time_start,time_end,time_diff,final_time;
    MPI_Barrier(MPI_COMM_WORLD);
    time_start=MPI_Wtime();

    switch (AND)//different cases will be executed based on the type of query
    {   case 0:{
        //OR case
        while((count<local_size)||!eof)//this loop runs untill either we have reached the EOF or read the process's region completely
        {   
            buf[0]='\0';
            int wlen=0;
            //storing the line number and the wordindex on the line of the current word          
            int w_line=line;
            int w_in=word_index;
            
            int end=get_word(fd,buf,&wlen,&line);
            buf[wlen]='\0';
            
            //updating the line number and word index for the next word that will be read in the next iteration
            if(w_line!=line)
            word_index=1;
            else
            ++word_index;
            //checking whether the word matches with any of the query word
            for(int i=0;i<Q_size;++i)
            {
                if(strcmp(q_word[i],buf)==0)
                {
                 add_word(&q[i],w_in,w_line,i);
                 
                }
            }
             if(end<0)
            {
            count=count+-1*end;
           
            continue;
            
            }
            count+=end;
         
    
        }
        //OR case finished
        break;
        }
        case 1://AND case
        {  NODE *head;
           NODE *tail; 
           head=(NODE *)malloc(sizeof(NODE));
           NODE *t=head;
            for(int i=0;i<Q_size;++i)//storing the first Q_size no. of words in a linked list 
            {
              buf[0]='\0';
              int wlen=0;
              int w_line=line;
              int w_in=word_index;
              int end=get_word(fd,buf,&wlen,&line);
              buf[wlen]='\0';
              if(w_line!=line)
                word_index=1;
              else
                ++word_index;
              
              t->word=(char *)malloc(sizeof(char)*(wlen+1));
              t->lno=w_line;
              t->win=w_in;
              strcpy(t->word,buf);
              t->wlen=wlen;
              if(i!=Q_size-1)
              {
                  t->next=(NODE *)malloc(sizeof(NODE));
                  t=t->next;
              }
              else
              {t->next=NULL;
              tail=t;}
              if(end<0)
                {
                 count=count+-1*end;
                 break;
                }
                count+=end;
            }
            
            while((count<local_size)||!eof)
            {
                if(check(head,q_word,Q_size))
                {add_word(&q[0],head->win,head->lno,0);//comparing the contents of the linked list with the query
                }
                
              // reading the word and updating the linked list
                buf[0]='\0';
                int wlen=0;
                int w_line=line;
                int w_in=word_index;
                int end=get_word(fd,buf,&wlen,&line);
                buf[wlen]='\0';
              if(w_line!=line)
                word_index=1;
              else
                ++word_index;

                NODE *t=(NODE *)malloc(sizeof(NODE));
                 t->word=(char *)malloc(sizeof(char)*(wlen+1));
                 t->lno=w_line;
                 t->win=w_in;
                 strcpy(t->word,buf);
                 t->wlen=wlen;
                 tail->next=t;
                 tail=t;
                 NODE *f=head;
                 head=head->next;
                 free(f);

                     if(end<0)
                    {
                    count=count+-1*end;
                    continue;
                    }
                    count+=end;

            }
             int extra=1;
             int temp_line=line;
             int temp_w_ind=word_index;
             //we will have to check the case where some words lie in the region of another process
             while(extra<=Q_size)
             {
                 if(check(head,q_word,Q_size))
                  add_word(&q[0],head->win,head->lno,0);
                
                 buf[0]='\0';
                int wlen=0;
                int w_line=temp_line;
                int w_in=temp_w_ind;
                int end=get_word(fd,buf,&wlen,&temp_line);
                buf[wlen]='\0';

                if(w_line!=temp_line)
                 temp_w_ind=1;
                else
                 ++temp_w_ind;

                 NODE *t=(NODE *)malloc(sizeof(NODE));
                 t->word=(char *)malloc(sizeof(char)*(wlen+1));
                 t->lno=w_line;
                 t->win=w_in;
                 strcpy(t->word,buf);
                 t->wlen=wlen;
                 tail->next=t;
                 tail=t;
                 NODE *f=head;
                 head=head->next;
                 free(f);
                    
                 ++extra;
                 if(end<0)
                 {
                     if(check(head,q_word,Q_size))
                  add_word(&q[0],head->win,head->lno,0);

                  break;
                 }
             }
                
             
          break;
        }

    }
    time_end=MPI_Wtime();
    time_diff=time_end-time_start;
    
    MPI_Reduce(&time_diff,&final_time,1,MPI_DOUBLE,MPI_MAX,0,MPI_COMM_WORLD);
    

    int w_recv,line_recv;
    /* getting the no. of lines read till the previous process so that we can get the global index of the word in the file.
    Till now we only knew the index relative to the region assigned to the process*/
    if(num_tasks>1)
    MPI_Scan(&line,&line_recv,1,MPI_INT,MPI_SUM,MPI_COMM_WORLD);
    if(rank==0&&num_tasks>1)
    {
        MPI_Send(&word_index,1,MPI_INT,1,0,MPI_COMM_WORLD);
    }
    else if(rank!=0&&num_tasks>1)
    {
        MPI_Recv(&w_recv,1,MPI_INT,rank-1,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
        if(rank!=num_tasks-1)
        {
          MPI_Send(&word_index,1,MPI_INT,rank+1,0,MPI_COMM_WORLD);  
        }
    }
    if(rank!=0)
    {
     line_recv=line_recv-line;
    }
    
    int pword,pline;
    int *lno_send;
    int *win_send;
    //calculating and sending the results to process 0
    if(rank!=0)
    {   
        for(int i=0;i<qsz;++i)
        {  lno_send=(int *)malloc(sizeof(int)*q[i].ind);
           win_send=(int *)malloc(sizeof(int)*q[i].ind);
            for(int j=0;j<q[i].ind;++j)
            {  pline=line_recv+1;
              if(q[i].ln[j]==0)
                {
        
                    pword=q[i].win[j]+w_recv-1;
                }
                else
                {   pword=q[i].win[j];
                    pline+=q[i].ln[j];
                }
                 
                lno_send[j]=pline;
                win_send[j]=pword;

            }
            \
            MPI_Send(&q[i].ind,1,MPI_INT,0,0,MPI_COMM_WORLD);
            MPI_Send(lno_send,q[i].ind,MPI_INT,0,0,MPI_COMM_WORLD);
            MPI_Send(win_send,q[i].ind,MPI_INT,0,0,MPI_COMM_WORLD);
           
            free(lno_send);
            free(win_send);
    
        }
    
        
        
    }
    else//process 0 recieving and printing all the results
    {   for(int i=0;i<qsz;++i)
       {     lno_send=(int *)malloc(sizeof(int)*100);
             win_send=(int *)malloc(sizeof(int)*100);
             if(!AND)
             printf("Word=%s\n...\n",q_word[i]);
             else
             {printf("Word=");
                 for(int j=0;j<Q_size;++j)
                  printf("%s ",q_word[j]);

                  printf("\n...\n");
             }
              for(int j=0;j<q[i].ind;++j)
              {
                  printf("line no.=%d, word_index=%d\n",q[i].ln[j]+1,q[i].win[j]);
              }
            for(int j=1;j<num_tasks;++j)
            { int sz;
             MPI_Recv(&sz,1,MPI_INT,j,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
             MPI_Recv(lno_send,100,MPI_INT,j,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
             MPI_Recv(win_send,100,MPI_INT,j,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
            
             for(int k=0;k<sz;++k)
             {
                 printf("line no.=%d word index=%d\n",lno_send[k],win_send[k]);
             }
            }
            printf("\n...\n");
             free(lno_send);
             free(win_send);
       }
    }
    
    // if(rank==0)
    // printf("Max time=%f\n",final_time);
    MPI_Finalize();

}
