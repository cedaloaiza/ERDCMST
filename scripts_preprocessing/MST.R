nombreArchivo <- "files/spainNormIds.txt"
nombreArchivo <- "halfSpainNormIds.txt"
nombreArchivo <- "files/spainOneAndAHalfNormIds.txt"

nombreArchivo <- "halfSpainNormIdsFake.txt"

points <- read.csv(nombreArchivo, header=TRUE, sep=",",
                   row.names=NULL)
#points <- points[1:20,]
points$ID <- NULL
points$NAME <- NULL

#points <- points[1:20,]
                 
#points$X <- round(points$X, 2)
#points$Y <- round(points$Y, 2)
#points$Y.COORD <- round(points$Y.COORD, 5)

distances <- dist(points)
distM <- as.matrix(distances)
distM <- round(distM, 5)
initialCost <- sum(distM[1,])
rm(distances)
gc()

#head(distM)
library("igraph")
g <-graph_from_adjacency_matrix(distM, mode="undirected", weighted=T)

#rm(distM)
gc()


mstR <- mst(g)
write_graph(mstR, "mst_small", format = "pajek")

sum(E(mstR)$weight)


dists <- distances(mstR, v=1)
max(dists)
removing <- c()
lambda <- 0.8
for (i in 1:length(dists)) {
  if(dists[i] > lambda) {
    removing <- c(removing, i)
  }
}

reconnectingCost <- sum(distM[1,removing])

newMST <- delete_vertices(mstR, removing)
mstCostAfterRemove <- sum(E(newMST)$weight)
mstReconnected <- mstCostAfterRemove + reconnectingCost


edgelistFileConn = file("edgelists_reapired_mst_spain_complete", "a")
length(removing)
for (i in removing) {
  write(paste(1, i, distM[1,i]), edgelistFileConn)
}
#close(edgelistFileConn)

#write_graph(newMST, "edgelists_reapired_mst_spain_complete_remaining", format = "pajek")


edges <- get.edgelist(newMST)
nrow(edges)
for (i in 1:nrow(edges)) {
  #x <- as.numeric(edges[i,1])
  #y <- as.numeric(edges(i,2))
  #print()
  #print(paste(edges[i,1], edges[i,2]))#, ))
  write(paste(edges[i,1], edges[i,2], distM[edges[i,1],edges[i,2]]), edgelistFileConn)
}
close(edgelistFileConn)
newMST
distM[13,14]
