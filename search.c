#include<mpi.h>
#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<sys/types.h>
#include<sys/stat.h>
#include<fcntl.h>
#include<unistd.h>
#define MAXLEN 100
int get_file_size(int fd)
{
    struct stat buf;
    fstat(fd,&buf);
    //printf("fsize=%d\n",(int)buf.st_size);
    return buf.st_size;
}
void set_pointer(int fd,int offset)
{
    lseek(fd,offset,SEEK_SET);
}
void correct(int fd,int *line)
{
    lseek(fd,-1,SEEK_CUR);
    char c;
    read(fd,&c,1);
    
    if(c==' '||c=='\n')
    return;
    int x=read(fd,&c,1);
    if(x==-1)
    return;
    while(c!=' '&&c!='\n')
    {    
        x=read(fd,&c,1);
        if(x==-1)
        return;
    }
    if(c=='\n')
    (*line)=(*line)+1;
}


int get_word(int fd,char *buf,int *wlen,int *line)
{
    char c;
    int x=read(fd,&c,1);
    if(x==0)
     return -1;
     int count=1;
     int ptr=0;
     buf[ptr++]=c;
     *wlen=*wlen+1;
     while(c!=' '&&c!='\n')
     {
       x=read(fd,&c,1);
       if(x==0)
        return -1*count;
       count=count+x;
       if(c>='a'&&c<='z'||c>='A'&&c<='Z')
         {buf[ptr++]=c;
         *wlen=*wlen+1;}

     }
     if(c=='\n')
     (*line)=(*line)+1;
     while(c==' '||c=='\n'||c==',')
     {x=read(fd,&c,1);
     if(x==0)
      return -1*count;
     if(c=='\n')
     (*line)=(*line)+1;
     ++count;}
     --count;
     lseek(fd,-1,SEEK_CUR);
     return count;
}


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
void add_word(POS *p,int win,int ln)
{
    p->win[p->ind]=win;
    p->ln[p->ind]=ln;
    p->ind=p->ind+1;
    if(p->ind==p->cap)
    {   p->cap=p->cap*2;
        p->ln=(int *)realloc(p->ln,sizeof(int)*p->cap);
        p->win=(int *)realloc(p->win,sizeof(int)*p->cap);
    }
}
int main(int argc, char*argv[] )
{
    int rank,num_tasks;
    MPI_Status Stat;
    MPI_Init(&argc,&argv);
    MPI_Comm_size(MPI_COMM_WORLD,&num_tasks);
    MPI_Comm_rank(MPI_COMM_WORLD,&rank);
    int fd=open(argv[1],O_RDONLY);
    int file_size=get_file_size(fd);
    
    int local_size=file_size/num_tasks;
    set_pointer(fd,local_size*rank);
    int line=0;
    if(rank!=0)
    correct(fd,&line);
    //printf("rank= %d, line=%d \n",rank,line);
    // char c;
    // read(fd,&c,1);
    int count=0;
    int word_index=1;
    char word[MAXLEN];
    strcpy(word,argv[2]);
    POS w1;
    pos_init(&w1,0);
   // printf("%s \n",word);
    while(count<local_size)
    {   char buf[MAXLEN];
        buf[0]='\0';
        int wlen=0;
        
        int w_line=line;
        int w_in=word_index;
        int end=get_word(fd,buf,&wlen,&line);
        buf[wlen]='\0';
       // if()
        if(w_line!=line)
        word_index=1;
        else
        ++word_index;
        if(strcmp(word,buf)==0)
        {
          add_word(&w1,w_in,w_line);
          //printf("rank=%d, word=%s ,win=%d, ln=%d \n",rank,word,w1.win[w1.ind-1],w1.ln[w1.ind-1]);
        }
        if(end<0)
        {
            count=count+-1*end;
            //printf("rank= %d, count=%d \n",rank,count);
            
            printf("rank= %d, first_word=%s line=%d \n",rank,buf,line);
            break;
        }
        count+=end;
        //printf("rank= %d, count=%d \n",rank,count);
        //buf[wlen]='\0';
        printf("rank= %d, first_word=%s wlen=%d \n",rank,buf,wlen);
    
    }
    
    

    MPI_Finalize();

}
