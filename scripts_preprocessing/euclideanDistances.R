nombreArchivo <- "files/spain_locs.csv"


points <- read.csv(nombreArchivo, header=TRUE, sep=",",
                          row.names=NULL)
points$ID <- NULL
points$NAME <- NULL

#points$X.COORD <- round(points$X.COORD, 2)
#points$Y.COORD <- round(points$Y.COORD, 2)

#distances <- dist(points)

maxX <- max(points$X.COORD)
minX <- min(points$X.COORD)
xRange <- abs(maxX - minX)

points2 <- points


points2$X.COORD <- points2$X.COORD + xRange 

duplicatedPoints <- rbind(points, points2)

#Ddistances <- dist(duplicatedPoints)

maxY <- max(points$Y.COORD)
minY <- min(points$Y.COORD)
yRange <- abs(maxY - minY)

points3 <- duplicatedPoints

points3$Y.COORD <- duplicatedPoints$Y.COORD + yRange

cuadruplicatedPoints <- rbind(points3, duplicatedPoints) 


writePoints(cuadruplicatedPoints)

writePoints <- function(writingPoints){
  fileConn<-file("output.txt", "a")
  for (i in 1:nrow(writingPoints)) {
    line <- paste0("[",i-1,",",writingPoints[i,1],",",writingPoints[i,2],"]")
    #print(writingPoints[i,1])
    write(line, fileConn)
  }
  close(fileConn)
}

writePointsSin <- function(writingPoints){
  #fileConn<-file("output.txt", "a")
  fileConn<-file("files/spainNormIds.txt", "a")
  for (i in 1:nrow(writingPoints)) {
    line <- paste0(i-1,",",writingPoints[i,1],",",writingPoints[i,2])
    #print(writingPoints[i,1])
    write(line, fileConn)
  }
  close(fileConn)
}

