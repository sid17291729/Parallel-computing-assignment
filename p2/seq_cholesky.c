#include<stdio.h>
#include<math.h>
#include<stdlib.h>

void print_intro(){
    printf("Calculate Cholesky factor for symmetric square matrices.\n");
    printf("Enter the matrix in the following way:\n");
    printf("First row should have number of rows and cols in the square matrix [n]\n");
    printf("This must be followed by n lines with n numbers in each line. Eg:\n");
    printf("3\n1 2 4\n2 13 23\n4 23 77\nStart>>\n");
}

int**populate_matrix(int n){
    int**A;
    int x=0;;
    A = (int**)malloc(sizeof(int*)*n);
    for(int i=0;i<n;i++){
        A[i] = (int*)malloc(sizeof(int)*n);
        for(int j=0;j<n;j++){  
            scanf("%d", &x);
            A[i][j] = j>=i?x:0;
        }
    }
    return A;
}

void cholesky(int**A, int n){
    for(int k=0; k<n;k++){
        A[k][k] = sqrt(A[k][k]);
        for(int j=k+1; j<n;j++){
            A[k][j] = A[k][j]/A[k][k];
        }
        for(int i=k+1;i<n;i++){
            for(int j=i;j<n;j++){
                A[i][j] = A[i][j] - A[k][i]*A[k][j];
            }
        }
    }
}


void print_m(int**A, int n){
    for(int i=0; i<n;i++){
        for(int j=0;j<n;j++){
            printf("%4d ",A[i][j]);
        }
        printf("\n");
    }
    printf("-----------------------------\n");
}

int main(){
    print_intro();
    int n;
    scanf("%d", &n);
    int**A = populate_matrix(n);
    print_m(A,n);
    printf("SOLVING\n");
    cholesky(A,n);
    print_m(A,n);
}