import random


n = int(input("Dimensions?: "))
f = open("test_matrix.txt","w")
L = []
LT = []
A = []
for i in range(n):
    l_list = []
    lt_list = []
    a_list = []
    for k in range(n):
        l_list.append(0)
        lt_list.append(0)
        a_list.append(0)
    L.append(l_list)
    LT.append(lt_list)
    A.append(a_list)

for i in range(n):
    for j in range(n):
        if i<j:
            continue
        L[i][j] = random.randint(1,9)
        LT[j][i] = L[i][j]
f.write(str(n)+'\n')
for i in range(n):
    for j in range(n):
        for k in range(n):
            A[i][j]+=L[i][k]*LT[k][j]
        f.write(str(A[i][j])+" ")
    f.write('\n')
f.write("\n\n\n")
for i in range(n):
    for j in range(n):
        f.write(str(L[i][j])+" ")
    f.write("\n")
# print(L)