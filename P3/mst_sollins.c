#include <limits.h>
#include <math.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <mpi.h>

const int UNSET_ELEMENT = -1;

typedef struct Set {
	int elements;
	int* parent;
	int* rank;
} Set;

typedef struct WeightedGraph {
	int edges;
	int vertices;
	int* edgeList;
} WeightedGraph;

void newWeightedGraph(WeightedGraph* graph, const int vertices, const int edges) {
	graph->edges = edges;
	graph->vertices = vertices;
	graph->edgeList = (int*) calloc(edges * 3, sizeof(int));
}

void readGraphFile(WeightedGraph* graph, const char inputFileName[]) {
	// open the file
	FILE* inputFile;
	const char* inputMode = "r";
	inputFile = fopen(inputFileName, inputMode);
	if (inputFile == NULL) {
		fprintf(stderr, "Couldn't open input file, exiting!\n");
		exit(EXIT_FAILURE);
	}

	int fscanfResult;

	// first line contains number of vertices and edges
	int vertices = 0;
	int edges = 0;
	fscanfResult = fscanf(inputFile, "%d %d", &vertices, &edges);
	newWeightedGraph(graph, vertices, edges);

	int from;
	int to;
	int weight;
	for (int i = 0; i < edges; i++) {
		fscanfResult = fscanf(inputFile, "%d %d %d", &from, &to, &weight);
		graph->edgeList[i * 3] = from;
		graph->edgeList[i * 3 + 1] = to;
		graph->edgeList[i * 3 + 2] = weight;

		if (fscanfResult == EOF) {
			fprintf(stderr,"Something went wrong during reading of graph file, exiting!\n");
			fclose(inputFile);
			exit(EXIT_FAILURE);
		}
	}

	fclose(inputFile);
}

void printWeightedGraph(const WeightedGraph* graph) {
	printf("------------------------------------------------\n");
	for (int i = 0; i < graph->edges; i++) {
		for (int j = 0; j < 3; j++) {
			printf("%d\t", graph->edgeList[i * 3 + j]);
		}
		printf("\n");
	}
	printf("------------------------------------------------\n");
}

void newSet(Set* set, const int elements) {
	set->elements = elements;
	set->parent = (int*) malloc(elements * sizeof(int)); // maintain parent 
	memset(set->parent, UNSET_ELEMENT, elements * sizeof(int));
	set->rank = (int*) calloc(elements, sizeof(int)); //  maintain rank
}

int findSet(const Set* set, const int vertex) {
	if (set->parent[vertex] == UNSET_ELEMENT) {
		return vertex;
	} 
	else {
		set->parent[vertex] = findSet(set,set->parent[vertex]);
		return set->parent[vertex];
	}
}

void unionSet(Set* set, const int parent1, const int parent2) {
	int root1 = findSet(set, parent1);
	int root2 = findSet(set, parent2);

	if (root1 == root2) {
		return;
	} 
	// Attach smaller rank tree under root of high
	else if (set->rank[root1] < set->rank[root2]) {
		set->parent[root1] = root2;
	} 
	else if (set->rank[root1] > set->rank[root2]) {
		set->parent[root2] = root1;
	} 
	// If ranks are same, then make one as root and 
    // increment its rank by one
	else {
		set->parent[root1] = root2;
		set->rank[root2] = set->rank[root1] + 1;
	}
}

void copyEdge(int* to, int* from) {
	memcpy(to, from, 3 * sizeof(int));
}

void deleteSet(Set* set) {
	free(set->parent);
	free(set->rank);
}

//cleanup graph data
void deleteWeightedGraph(WeightedGraph* graph) {
	free(graph->edgeList);
}

void distrbuteEdges(int* edgeList,int* edgeList_pp,int nOEdges){
    int rank,size;
}

void mst_sollins(const WeightedGraph* graph,WeightedGraph* mst){
    int rank,size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Status stat;

    bool isParallel=(size!=1);

    int nOEdges=graph->edges;
    int nOVertices=graph->vertices;
    
    MPI_Bcast(&nOEdges, 1, MPI_INT, 0, MPI_COMM_WORLD);
	MPI_Bcast(&nOVertices, 1, MPI_INT, 0, MPI_COMM_WORLD);
    //printf("%d %d %d\n",nOVertices,nOEdges,rank);
    MPI_Barrier(MPI_COMM_WORLD);
    //No Error Checkpoint

    //Distributing edge list amongst processes
    //*START*
    int myCount=0;
    int *myEdgeList=NULL;
    if(rank==0){
        myCount=nOEdges/size;
        if(rank<nOEdges/size)
            myCount++;
        myEdgeList= (int*)malloc(myCount*3*sizeof(int));
        int *disp=(int*)malloc(size*sizeof(int));
        int *count=(int*)malloc(size*sizeof(int));
        count[0]=myCount*3;
        disp[0]=0;
        for(int i=1;i<size;i++){
            if(i<nOEdges%size){
                count[i]=myCount*3;
            }
            else{
                count[i]=(myCount-1)*3;
            }
            disp[i]=disp[i-1]+count[i-1];
            //printf("%d %d\n",count[i],disp[i]);
        }
        MPI_Scatterv(graph->edgeList,count,disp,MPI_INT,myEdgeList,myCount*3,MPI_INT,0,MPI_COMM_WORLD);
    }
    else{
        myCount=nOEdges/size;
        if(rank<nOEdges/size)
            myCount++;
        myEdgeList= (int*)malloc(myCount*3*sizeof(int));
        MPI_Scatterv(NULL, NULL, NULL, MPI_INT,myEdgeList,myCount*3,MPI_INT,0,MPI_COMM_WORLD);        
    }
    //*END*
    //No Error Checkpoint
    //printf("done1\n");

    long double start = MPI_Wtime();
    Set* set = &(Set ) { .elements = 0, .parent = NULL, .rank =NULL };
	newSet(set, nOVertices);

    int mstEdges = 0;
	int* myClosestEdge = (int*)malloc((nOVertices * 3)* sizeof(int));
	int* closestEdgeRecieved;
	if (isParallel) {
		closestEdgeRecieved = (int*) malloc(nOVertices * 3 * sizeof(int));
	}
    //No Error Checkpoint
    //printf("done2\n");

	for (int i = 1; i < nOVertices && mstEdges < nOVertices - 1; i *= 2) {
		// reset all closestEdge
        //printf("%d %d\n",i,rank);
        MPI_Barrier(MPI_COMM_WORLD);
		for (int j = 0; j < nOVertices; j++) {
			myClosestEdge[j*3+2] = INT_MAX;
        }
		// find closestEdge
		for (int j = 0; j < myCount; j++) {
			int* currentEdge = &myEdgeList[j * 3];
			int root1 =findSet(set, currentEdge[0]),root2= findSet(set, currentEdge[1]);

			// eventually update closestEdge
			if (root1 != root2) {
                if(currentEdge[2]<myClosestEdge[root1*3+2]){
                    myClosestEdge[root1*3]=currentEdge[0];
                    myClosestEdge[root1*3+1]=currentEdge[1];
                    myClosestEdge[root1*3+2]=currentEdge[2];
                }
                if(currentEdge[2]<myClosestEdge[root2*3+2]){
                    myClosestEdge[root2*3]=currentEdge[0];
                    myClosestEdge[root2*3+1]=currentEdge[1];
                    myClosestEdge[root2*3+2]=currentEdge[2];
                }
			}
		}
        //No Error Checkpoint

        MPI_Barrier(MPI_COMM_WORLD);

		if (isParallel) {
			int from;
			int to;
			for (int step = 1; step < size; step *= 2) {
				if (rank % (2 * step) == 0) {
					from = rank + step;
					if (from < size) {
						MPI_Recv(closestEdgeRecieved, nOVertices * 3,MPI_INT,from,0,MPI_COMM_WORLD,&stat);

						// combine all closestEdge parts
						for (int i = 0; i < nOVertices; i++) {
							int v = i * 3;
							if (closestEdgeRecieved[v+2]<myClosestEdge[v+2]) {
								myClosestEdge[v]=closestEdgeRecieved[v];
                                myClosestEdge[v+1]=closestEdgeRecieved[v+1];
                                myClosestEdge[v+2]=closestEdgeRecieved[v+2];
							}
						}
					}
				} 
				else if (rank % step == 0) {
					to = rank - step;
					MPI_Send(myClosestEdge,nOVertices*3,MPI_INT, to,0,MPI_COMM_WORLD);
				}
			}
			// publish all closestEdge parts
			MPI_Bcast(myClosestEdge, nOVertices * 3, MPI_INT, 0,MPI_COMM_WORLD);
		}
            // add new edges to MST
        for (int j = 0; j <nOVertices; j++) {
            if (myClosestEdge[j*3+2] != INT_MAX) {
                int from = myClosestEdge[j * 3];
                int to = myClosestEdge[j * 3 + 1];
                //printf("from %d to %d\n",from,to);
                // prevent adding the same edge twice
                if (findSet(set, from) != findSet(set, to)) {
                    if (rank == 0) {
                        copyEdge(&mst->edgeList[mstEdges * 3],&myClosestEdge[j * 3]);
                    }
                    mstEdges++;
                    unionSet(set, from, to);
                }
            }
        }
	}
    long double maxtime=0;
    start=MPI_Wtime()-start;
    MPI_Reduce(&start,&maxtime,1,MPI_LONG_DOUBLE,MPI_MAX,0,MPI_COMM_WORLD);
    if(rank==0)
        printf("Time elapsed: %.12Lf s\n", maxtime);
	// clean up
	deleteSet(set);
	free(myClosestEdge);
	if (isParallel) {
		free(closestEdgeRecieved);
		free(myEdgeList);
	}
}

int main(int argc, char* argv[]) {
	// MPI variables and initialization
	int rank;
	int size;
	MPI_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &size);
	// graph Variables
	WeightedGraph* graph = &(WeightedGraph ) { .edges = 0, .vertices = 0,.edgeList = NULL };
	WeightedGraph* mst = &(WeightedGraph ) { .edges = 0, .vertices = 0,	.edgeList = NULL };
	if (rank == 0) {
		// read the maze file and store it in the graph
		readGraphFile(graph, argv[1]);
		// print the edges of the read graph
		printf("Original Graph:\n");
		printWeightedGraph(graph);
		newWeightedGraph(mst, graph->vertices, graph->vertices - 1);
	}
	
	// use Boruvka's algorithm
	mst_sollins(graph, mst);
	if (rank == 0) {		
		// print the edges of the MST
		printf("Minimum Spanning Tree (Boruvka):\n");
		printWeightedGraph(mst);
		unsigned long weightMST = 0;
		for (int i = 0; i < mst->edges; i++) {
			weightMST += mst->edgeList[i * 3 + 2];
		}
		printf("MST weight: %lu\n", weightMST);
		// cleanup
		deleteWeightedGraph(graph);
		deleteWeightedGraph(mst);
	}
    //start=MPI_Wtime()-start;
    //printf("Time elapsed=%f",start);
	MPI_Finalize();
	return EXIT_SUCCESS;
}