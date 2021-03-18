#include<stdio.h>
#include<mpi.h>
#include<stdlib.h>
#include<math.h>

#define ROW_SEND_RECIEVE_TAG 1

void print_intro();
double** populate_matrix(int n);
void print_m(double**,int);


int main(){
    int my_rank, comm_sz;
    MPI_Init(NULL,NULL);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &comm_sz);
    if(my_rank==0){
        double A[3] = {1,2,3};
        MPI_Send(A, 3, MPI_DOUBLE,1, ROW_SEND_RECIEVE_TAG, MPI_COMM_WORLD);
    }
    if(my_rank==1){
        double** local = (double**)malloc(sizeof(double*)*3);
        local[0] = (double*)malloc(sizeof(double)*3);
        int i=3;
        
        MPI_Recv(local,3,MPI_DOUBLE,0,ROW_SEND_RECIEVE_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        while(i--){
            printf("%lf\n", local[0][i]);
        }
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
