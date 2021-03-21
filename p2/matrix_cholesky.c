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
    double start, finish, loc_elapsed, elapsed;
    
    MPI_Init(NULL,NULL);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &comm_sz);
    /*---------GET INPUT MATRIX A----------*/
    if(my_rank==0){
        print_intro();
        scanf("%d", &n);
        A = populate_matrix(n);
        // print_m(A,n);
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
    start = MPI_Wtime();
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
        int row_num_A = loc_process_row*comm_sz+my_rank; //position of row in A
        cur_row[row_num_A] = sqrt(cur_row[row_num_A]);
        for(int i=row_num_A+1;i<n;i++){
            cur_row[i] = cur_row[i]/cur_row[row_num_A];
        }
        // send row to all processes
        for(int i=0; i<comm_sz;i++){
            if(i==my_rank) continue;
            MPI_Isend(cur_row, n, MPI_DOUBLE,i, ROW_PROCESS_TAG, MPI_COMM_WORLD, &null_req);
        }
        //perform subtraction for all remaining rows in buffer before recieving rows from latter processes
        for(int i=loc_process_row+1; i*comm_sz+my_rank<n;i++){
            int row_num = i*comm_sz+my_rank;
            for(int k=row_num;k<n;k++){
                local_buffer[i][k]-= cur_row[row_num]*cur_row[k];
            }
        }
        //recieve latter rows and process for it
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
    finish = MPI_Wtime();
    loc_elapsed = finish-start;
    MPI_Reduce(&loc_elapsed, &elapsed, 1, MPI_DOUBLE,MPI_MAX,0,MPI_COMM_WORLD);
    MPI_Barrier(MPI_COMM_WORLD);
    
    /*--------------COLLECT ALL PROCESSED ROWS------------------*/
    if(my_rank==0){
        int zero_rows = 0;
        int next_source = 0;
        for(int i=0; i<n;i++){
            if(next_source==0){
                A[i] = local_buffer[zero_rows++];
            }
            else{
                MPI_Recv(A[i], n, MPI_DOUBLE,next_source, ROW_SEND_RECIEVE_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            }
            next_source = (next_source+1)%comm_sz;
        }
    }
    else{
        for(int i=0; i*comm_sz+my_rank<n;i++){
            MPI_Send(local_buffer[i], n, MPI_DOUBLE, 0, ROW_SEND_RECIEVE_TAG, MPI_COMM_WORLD);
            free(local_buffer[i]);
        }
    }
    free(local_buffer);
    MPI_Barrier(MPI_COMM_WORLD);
    if(my_rank==0){
        print_m(A,n);
        printf("TIME TAKEN: %lf ms\n", elapsed*1000);
    }
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
            printf("%5.2f ",A[i][j]);
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
