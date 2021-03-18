#include<mpi.h>
#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<sys/types.h>
#include<sys/stat.h>
#include<fcntl.h>
#include<unistd.h>
#define MAXLEN 100

int eof;
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
void correct(int fd,int *line,int *count)
{
    lseek(fd,-1,SEEK_CUR);
    char c;
    read(fd,&c,1);
   // printf("c=%d\n",c);
    if(c==32||c==10||c==13)
    {   
        while(c==' '||c=='\n'||c==13)
       {   //if(c==10)
            //(*line)=(*line)+1;
            
           int x=read(fd,&c,1);
           if(x==0)
           return;
           (*count)=(*count)+1;

        //    if(c==10||c==13)
        //     printf("nl\n");
        //     // else if(c=='\n')
        //     // printf("nl\n");
        //     // else
        //     // printf("c=%c\n",c);
        //     //printf("c=%d\n",c);
 
        }
        lseek(fd,-1,SEEK_CUR);
        (*count)=(*count)-1;
        //printf("c=%c\n",c);
        return;
    }
    int x=read(fd,&c,1);
    if(x==-1)
    return;
    while(c!=' '&&c!='\n')
    {   (*count)=(*count)+1; 
        x=read(fd,&c,1);
        if(x==-1)
        return;
    }
    // if(c=='\n'||c==13)
    // (*line)=(*line)+1;
}


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
      //printf("c=%c\n",c);
       if(x==0)
       { eof=1;
        return -1*count;}
       count=count+x;
       if(c>='a'&&c<='z'||c>='A'&&c<='Z')
         {buf[ptr++]=c;
         *wlen=*wlen+1;}

     }
     if(c=='\n')
     (*line)=(*line)+1;
     while(c==' '||c=='\n'||c==13||c==',')
     {x=read(fd,&c,1);
     if(x==0){eof=1;
      return -1*count;}
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

int main(int argc, char*argv[] )
{
    int rank,num_tasks;
    MPI_Status Stat;
    MPI_Init(&argc,&argv);
    MPI_Comm_size(MPI_COMM_WORLD,&num_tasks);
    MPI_Comm_rank(MPI_COMM_WORLD,&rank);
    int fd=open(argv[1],O_RDONLY);
    int file_size=get_file_size(fd);
    //printf("file size=%d\n",file_size);
    int local_size=file_size/num_tasks;
    set_pointer(fd,local_size*rank);
    int line=0;
    int count=0;
    if(rank!=0)
    correct(fd,&line,&count);
    if(rank!=num_tasks-1)
    eof=1;
    //printf("count=%d\n",count);
    int word_index=1;
    
    
    int Q_size=argc-2;
    POS q[Q_size];
    char *q_word[Q_size];
    
    for(int i=0;i<Q_size;++i)
    {pos_init(&q[i],0);
      q_word[i]=(char*)malloc(sizeof(char)*MAXLEN);
     strcpy(q_word[i],argv[i+2]);
    }



    while((count<local_size)||!eof)
    {   char buf[MAXLEN];
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
        for(int i=0;i<Q_size;++i)
        {
            if(strcmp(q_word[i],buf)==0)
            {
             add_word(&q[i],w_in,w_line,i);
             //printf("rank=%d, word=%s ,win=%d, ln=%d \n",rank,q_word[i],q[i].win[q[i].ind-1],q[i].ln[q[i].ind-1]);
            }
        }
        if(end<0)
        {
            count=count+-1*end;
            //printf("rank= %d, wlen=%d \n",rank,wlen);
            
            //printf("rank= %d, first_word=%s line=%d \n",rank,buf,line);
            continue;
            //break;
        }
        count+=end;
        //printf("rank= %d, count=%d \n",rank,count);
        //buf[wlen]='\0';
        //printf("rank= %d, first_word=%s  end=%d \n",rank,buf,end);
        //printf("rank=%d, windex=%d\n",rank,word_index);
    
    }
    
    // if(rank==0)
    // {
    //     for(int i=0;i<num_tasks;++i)
    //     {
    //         MPI_SEND
    //     }
    // }
    int w_recv,line_recv;
    //MPI_Scan(&word_index,&w_recv,1,MPI_INT,MPI_SUM,MPI_COMM_WORLD);
    MPI_Scan(&line,&line_recv,1,MPI_INT,MPI_SUM,MPI_COMM_WORLD);
    if(rank==0)
    {
        MPI_Send(&word_index,1,MPI_INT,1,0,MPI_COMM_WORLD);
    }
    else
    {
        MPI_Recv(&w_recv,1,MPI_INT,rank-1,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
        if(rank!=num_tasks-1)
        {
          MPI_Send(&word_index,1,MPI_INT,rank+1,0,MPI_COMM_WORLD);  
        }
    }
    if(rank!=0)
    {//w_recv=w_recv-word_index;
     line_recv=line_recv-line;
    }
    //printf("rank =%d ,w_recv=%d, line_recv=%d\n",rank,w_recv,line_recv);
    int pword,pline;
    int *lno_send;
    int *win_send;
    if(rank!=0)
    {   
        for(int i=0;i<Q_size;++i)
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
                 //printf("rank =%d ,wno=%d, word_index=%d, line_no=%d\n",rank,q[i].wno,pword,pline);
                lno_send[j]=pline;
                win_send[j]=pword;

            }
            //printf("rank=%d, size=%d\n",rank,q[i].ind);
            MPI_Send(&q[i].ind,1,MPI_INT,0,0,MPI_COMM_WORLD);
            MPI_Send(lno_send,q[i].ind,MPI_INT,0,0,MPI_COMM_WORLD);
            MPI_Send(win_send,q[i].ind,MPI_INT,0,0,MPI_COMM_WORLD);
            //printf("rank=%d sent= %d\n",rank,s);
            free(lno_send);
            free(win_send);
    
        }
    
        
        
    }
    else
    {   for(int i=0;i<Q_size;++i)
       {     lno_send=(int *)malloc(sizeof(int)*100);
             win_send=(int *)malloc(sizeof(int)*100);
             printf("Word=%s\n...\n",q_word[i]);
              for(int j=0;j<q[i].ind;++j)
              {
                  printf("line no.=%d, word_index=%d\n",q[i].ln[j]+1,q[i].win[j]);
              }
            for(int j=1;j<num_tasks;++j)
            { int sz;
             MPI_Recv(&sz,1,MPI_INT,j,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
             MPI_Recv(lno_send,100,MPI_INT,j,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
             MPI_Recv(win_send,100,MPI_INT,j,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
            // printf("rank=%d recieved= %d\n",rank,rc);
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
    // if(w1.ind!=0)
    //     printf("rank =%d ,word_index=%d, line_no=%d\n",rank,pword,pline);

    //printf("rank =%d ,wrcv=%d, lircv=%d\n",rank,w_recv,line_recv);
    MPI_Finalize();

}