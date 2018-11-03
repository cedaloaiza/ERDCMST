'''
Created on Jan 28, 2014

@author: lquesada
'''

lengthUB=67.0
#lengthUB=90.0
#lengthUB=120.0
from math import sqrt
from math import pow
import networkx as nx
import time


def bkrus(source,sinks,D,ub,worst=False,nobound=False,verbose=True):
    V=[source]+sinks
    trees={}
    P={}
    P_={}
    r={}
    tp={}
    st=[]
    
    for x in V:
        trees[x]=set([x])
        tp[x]=x
        r[x]=0
        P[x]={}
        P_[x]={}
        for y in V:
            P[x][y]=0
            P_[x][y]=0
            
    def Merge(u,v):
        for x in trees[tp[u]]:
            for y in trees[tp[v]]:
                P[x][y]=P[y][x]=P[x][u]+D[u][v]+P[v][y]

        for x in trees[tp[u]]:
            r[x]=max([r[x]]+[P[x][i] for i in trees[tp[v]]])
#            if r[x]> lengthUB:
#                raise 'bla'
            
        for y in trees[tp[v]]:
            r[y]=max([r[y]]+[P[i][y] for i in trees[tp[u]]])
#            if r[y]> lengthUB:
#                raise 'bla'
            
        
    def Union(u,v):
        if tp[u]<tp[v]:            
            trees[tp[u]]|=trees[tp[v]]
            for x in trees[tp[v]]:
                tp[x]=tp[u]
        else:
            trees[tp[v]]|=trees[tp[u]]
            for x in trees[tp[u]]:
                tp[x]=tp[v]
        
                
        
    def checkUB(u,v):
        r_={}
        P_={}
        for x in V:
            r_[x]=r[x]
            P_[x]={}
            for y in V:
                P_[x][y]=P[x][y]

        for x in trees[tp[u]]:
            for y in trees[tp[v]]:
                P_[x][y]=P_[y][x]=P_[x][u]+D[u][v]+P_[v][y]

        for x in trees[tp[u]]:
            r_[x]=max([r_[x]]+[P_[x][i] for i in trees[tp[v]]])
            
        for y in trees[tp[v]]:
            r_[y]=max([r_[y]]+[P_[i][y] for i in trees[tp[u]]])
            
        if (source in trees[tp[u]]) or (source in trees[tp[v]]):  
            return (r_[source] <= ub)
            #return (P_[source][u]+D[u][v]+r_[v] <= ub)
        else:
            for x in trees[tp[u]]|trees[tp[v]]:
                if D[source][x] + r_[x] <= ub:
                    return True
            return False
    edges=sorted([(u,v) for u in V for v in V if u!=v],key= lambda (u,v):(-D[u][v] if worst else D[u][v]))    
    for it,(u,v) in enumerate(edges):
        if verbose:
            if (it%1000==0):
                print (it/float(len(edges)))*100,(max(len(trees[v]) for v in V)/float(len(V)))*100
        if tp[u] != tp[v]:
            if nobound or checkUB(u,v):
                Merge(u,v)
                Union(u,v)
                st.append((u,v))
#    print r

    DG=nx.DiGraph()
    for (i,j) in st:
        DG.add_edge(i,j,{'weight':D[i][j]})
        DG.add_edge(j,i,{'weight':D[j][i]})

    if not(nobound):
        pls= nx.shortest_path_length(DG,source=source,weight='weight')
        for x in pls:
            if pls[x]>ub:
                print 'length ERROR',x,pls[x]
        if len(pls)!=len(V):
            print 'cover ERROR',len(pls),len(V)
    return {'cost':sum(D[i][j] for (i,j) in st),'E':[(i,j,D[i][j]) for (i,j) in st]}

def getMetrosAndEssIrelandNewInstances(indir,primAndsecFile):
    f=open(indir+primAndsecFile)
    mns={}
    ess={}
    counter=0
    for line in f.readlines():
        [es,mn1,mn2]=map(int,line.split()[:3])
        ess[es]=(mn1,mn2)
        if mn1 in mns:
            mns[mn1].append(es)
        else:
            mns[mn1]=[es]
        if mn2 in mns:
            mns[mn2].append(es)
        else:
            mns[mn2]=[es]
        counter+=1
    return mns,ess

def computeDist(p1,p2):
    return sqrt(pow(p1[0]-p2[0],2)+pow(p1[1]-p2[1],2))/1000.0

def getMetroDistMatrix(coords):
    d=dict((i,{}) for i in coords)
    for i in coords:
        for j in coords:
            d[i][j]=computeDist(coords[i],coords[j])
    return d


def getMetroDistMatrices(mns,mns_coords,ess_coords):
    dist_matrices={}
    metro_coords={}
    for mn in mns:
        coords=dict([('m'+str(mn), mns_coords[mn])]+[(es,ess_coords[es]) for es in  mns[mn]])
        metro_coords[mn]=coords
        dist_matrices[mn]=getMetroDistMatrix(coords)
    return dist_matrices,metro_coords

 




def solveFullBMST(out_dir,source,ess_coords,ub,n):
    D={}
    for i in range(n):
        D[i]={}
        for j in range(n):
            D[i][j]=computeDist(ess_coords[i],ess_coords[j])
    print 'distance matrix created'
    start=time.clock()
    output=bkrus(source,[x for x in range(n) if x != source],D,ub)
    end=time.clock()-start
    f=open(out_dir+'BMST_'+str(n)+'_'+str(source)+'_'+str(ub)+'.txt','w')
    f.write(str(output['cost'])+'\n')
    totalD=0
    for (i,j) in output['E']:
        f.write(str(i)+'\t'+str(j)+'\t'+str(D[i][j])+'\n')
        totalD+=D[i][j]
    print 'cost',totalD
    f.close()
    return end
        
#in_dir='/Users/lquesada/Documents/CTVR/Papers/2014/CPAIOR/data/Ireland/'
#ess_coords=[map(float,line.split()[:2]) for line in open(in_dir+'ire_data_100_cover.txt').readlines()[1:] ]
#for n in [400]:#[500,600,700,800,900]:
#    source=147
#    ub=415
#    print n,' nodes'
#    print solveFullBMST(in_dir,source,ess_coords,ub,n)




source=0
sinks=[1,2,3,4]         

D=[[10,10,10,10,10],
   [10,1,1,1,1],
   [10,1,1,1,1],
   [10,1,1,1,1],
   [10,1,1,1,1]]  
D=[[0,10,15,9,5],
   [100,0,8,6,5],
   [100,12,0,1,3],
   [100,7,4,0,9],
   [100,6,1,1,0]]     
ub=13
print bkrus(source,sinks,D,ub)

