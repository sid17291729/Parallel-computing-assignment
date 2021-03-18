#include<stdio.h>
#include<mpi.h>
#include<stdlib.h>
#include<math.h>

#define ROW_SEND_RECIEVE_TAG 1
#define ROW_PROCESS_TAG 2
MPI_Request null_req = MPI_REQUEST_NULL;

void print_intro();
double** populate_matrix(int n);
void print_m(double**,int);


int main(){
    int n = 0, num_indv_rows;
    double **A, **local_buffer;
    int my_rank, comm_sz;
    
    MPI_Init(NULL,NULL);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &comm_sz);
    /*---------GET INPUT MATRIX A----------*/
    if(my_rank==0){
        print_intro();
        scanf("%d", &n);
        A = populate_matrix(n);
        print_m(A,n);
    }
    MPI_Bcast(&n, 1, MPI_INT, 0, MPI_COMM_WORLD); // broadcast n to all processes
    
    /*----------- SENDING INPUT TO ALL PROCESSES ------------------*/
    num_indv_rows = n/comm_sz+1;
    if(my_rank==0){
        local_buffer = (double**)malloc(sizeof(double*)*num_indv_rows);
        int zero_rows=0;
        int next_dest = 0;
        for(int current_row=0; current_row<n;current_row++){
            if(next_dest==0){
                local_buffer[zero_rows++] = A[current_row];
            }
            else{
                MPI_Send(A[current_row], n, MPI_DOUBLE,next_dest, ROW_SEND_RECIEVE_TAG, MPI_COMM_WORLD);
            }
            next_dest = (next_dest+1)%comm_sz;
        }
    }
    else{
        local_buffer = (double**)malloc(sizeof(double*)*num_indv_rows);
        for(int i=0; i*comm_sz+my_rank<n;i++){      //check if next row to be recieved is within limits or not
            local_buffer[i] = (double*)malloc(sizeof(double)*n);
            MPI_Recv(local_buffer[i], n,MPI_DOUBLE,0,ROW_SEND_RECIEVE_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        }
    }
    MPI_Barrier(MPI_COMM_WORLD);
    
    /*-----------------Cholesky decomposition------------------*/
    double*prev_row_recieve = (double*)malloc(sizeof(double)*n);
    int loc_process_row = 0; // current row number waiting to be processed in local buffer
    while(loc_process_row*comm_sz+my_rank<n){
        // recieve prev rows and perform subtractions for elements of the lower traingle
        for(int i=0; i<my_rank;i++){
            MPI_Recv(prev_row_recieve,n,MPI_DOUBLE,i, ROW_PROCESS_TAG, MPI_COMM_WORLD,MPI_STATUS_IGNORE);
            for(int j=loc_process_row; j*comm_sz+my_rank<n;j++){
                int row_num = j*comm_sz+my_rank; //row number in original matrix A
                for(int k=row_num;k<n;k++){
                    local_buffer[j][k]-=prev_row_recieve[row_num]*prev_row_recieve[k];
                }
            }
        }
        // process current row
        double *cur_row = local_buffer[loc_process_row];
        int row_num = loc_process_row*comm_sz+my_rank;
        cur_row[row_num] = sqrt(cur_row[row_num]);
        for(int i=cur_row+1;i<n;i++){
            cur_row[i] = cur_row[i]/cur_row[row_num];
        }
        // send row to all processes
        for(int i=0; i<comm_sz;i++){
            if(i==my_rank) continue;
            MPI_Isend(cur_row, n, MPI_DOUBLE, ROW_PROCESS_TAG, MPI_COMM_WORLD, &null_req);
        }
        //recieve later rows and process for it
        for(int i=my_rank+1;i<comm_sz&&loc_process_row*comm_sz+i<n;i++){
            MPI_Recv(prev_row_recieve,n,MPI_DOUBLE,i, ROW_PROCESS_TAG, MPI_COMM_WORLD,MPI_STATUS_IGNORE);
            for(int j=loc_process_row+1; j*comm_sz+my_rank<n;j++){
                int row_num = j*comm_sz+my_rank; //row number in original matrix A
                for(int k=row_num;k<n;k++){
                    local_buffer[j][k]-=prev_row_recieve[row_num]*prev_row_recieve[k];
                }
            }
        }
        loc_process_row++;
    }
    MPI_Barrier(MPI_COMM_WORLD);
    
    /*--------------collect all processed rows------------------*/
    int 
    for(int i=0; i<)

    MPI_Finalize();
    return 0;
}


double**populate_matrix(int n){
    double**A;
    double x=0;
    A = (double**)malloc(sizeof(double*)*n);
    for(int i=0;i<n;i++){
        A[i] = (double*)malloc(sizeof(double)*n);
        for(int j=0;j<n;j++){  
            scanf("%lf", &x);
            A[i][j] = j>=i?x:0;
        }
    }
    return A;
}

void print_m(double**A, int n){
    for(int i=0; i<n;i++){
        for(int j=0;j<n;j++){
            printf("%3.2f ",A[i][j]);
        }
        printf("\n");
    }
    printf("-----------------------------\n");
}

void print_intro(){
    printf("Calculate Cholesky factor for symmetric square matrices.\n");
    printf("Enter the matrix in the following way:\n");
    printf("First row should have number of rows and cols in the square matrix [n]\n");
    printf("This must be followed by n lines with n numbers in each line. Eg:\n");
    printf("3\n1 2 4\n2 13 23\n4 23 77\nStart>>\n");
}
