# making a subset of iris with species column
irisSubSetWithSpecies <- subset(iris) 
# grabing the same subset of iris with no species column for kmeans
irisSubSetWithNoSpecies <- irisSubSetWithSpecies[,-5] 




# calculating kmeans with different k to test best f measure
kmeans2 = kmeans(irisSubSetWithNoSpecies, 2) 
kmeans3 = kmeans(irisSubSetWithNoSpecies, 3)
kmeans4 = kmeans(irisSubSetWithNoSpecies, 4)
kmeans5 = kmeans(irisSubSetWithNoSpecies, 5)
kmeans7 = kmeans(irisSubSetWithNoSpecies, 7)
kmeans9 = kmeans(irisSubSetWithNoSpecies, 9)
kmeans11 = kmeans(irisSubSetWithNoSpecies, 11)




# saving kmeans in a list so that we can use a for loop
kmeansArray = list(kmeans2, kmeans3, kmeans4, kmeans5, kmeans7, kmeans9, kmeans11)




# function that takes a kmeans object as input and returns a list with names for the k clusters
getClusterNamesToNumberMap <- function(kmeans_) {
  # from a every row of this matrix we can know the name of the cluster by getting the highest count in the row
  table_ <- table(kmeans_$cluster, iris$Species) 
  matrix_ <- as.data.frame.matrix(table_)
  
  #saving space for the output matrix which will be equal to the number of rows in the kmeans object
  numberOfRows <- nrow(matrix_)
  outputMatrix <- matrix(nrow = numberOfRows, ncol = 2)
  
  # this for loop goes through every row
  # finds the cluster with more objects and saves the name of the column
  # in the output matrix because columns are the name of the clusters
  for (rowIndex in 1:numberOfRows) {
    row <- matrix_[rowIndex,]
    greatestCell.value <- max(row)
    greatestCell.index = match(greatestCell.value, row)
    greatestCell.name <- names(row[greatestCell.index])
    outputMatrix[rowIndex, 1] <- rowIndex
    outputMatrix[rowIndex, 2] <- greatestCell.name
  }
  
  #returning a list of cluster names which match the numbers in the kmeans$cluster
  # this list only contains names to the k clusters, it is not the list that maps every
  # single row from the subset of iris to a predicted Cluster
  return(outputMatrix)
}




# takes a kmeans object and returns a dataframe with the clusterNames column appended at the end
# so that the real dataframe$Species taken from iris$Species and the dataframe$predictedSpecies 
# may be compared
getIrisWithClusterNames <- function(kmeans_, irisSubSetWithSpecies) {
  # get map object with kmeans cluster numbers to kmeans cluster names
  clusterNamesToNumberMap <- getClusterNamesToNumberMap(kmeans_)
  
  # appending the cluster number column to a subset of iris
  dataFrameWithClusterNumbers <- data.frame(irisSubSetWithSpecies, kmeansClusterNumber = kmeans_$cluster)
  
  # using a for loop to iterate through the rows in the kmeans object and map the cluster number to a cluster name
  numberOfRows <- nrow(dataFrameWithClusterNumbers)
  listOfClusterNames <- vector(length = numberOfRows)
  for (rowIndex in 1:numberOfRows) {
    clusterNumber <- dataFrameWithClusterNumbers$kmeansClusterNumber[rowIndex]
    nameColumn = 2
    listOfClusterNames[rowIndex] <- clusterNamesToNumberMap[clusterNumber,nameColumn]
  }
  
  # creating a dataframe from the iris SubSet with an appended column with cluster names
  irisSubSetWithClusterNames <- data.frame(irisSubSetWithSpecies, kmeansClusterName = listOfClusterNames)
  return(irisSubSetWithClusterNames)
}




# this function takes ainput a data frame that contains the iris subset with real species and predicted species from
# kmeans so that we can compute the f measure easily
getFmeasure <- function(irisSubSetWithClusterNames) {
  truePositives <- 0
  falsePositives <- 0
  trueNegatives <- 0
  falseNegatives <- 0
  
  species = unique(irisSubSetWithClusterNames$Species)
  for (specieIndex in 1:length(species)) {
    currentSpecie <- toString(species[specieIndex])
    for (rowIndex in 1:nrow(irisSubSetWithClusterNames)) {
      trueSpecie <- toString(irisSubSetWithClusterNames$Species[rowIndex])
      predictedSpecie <- toString(irisSubSetWithClusterNames$kmeansClusterName[rowIndex])
      
      if (currentSpecie == trueSpecie) { 
        if (currentSpecie == predictedSpecie) { 
          truePositives <- truePositives + 1
        } else { 
          falseNegatives <- falseNegatives + 1
        }
      } else { 
        if (currentSpecie == predictedSpecie) { 
          falsePositives <- falsePositives + 1
        } else { 
          trueNegatives <- trueNegatives + 1
        }
      }
    }
  }
  precision <- truePositives / (truePositives + falsePositives)
  recall <- truePositives / (truePositives + falseNegatives)
  fmeasure <- (2 * precision * recall) / (precision / recall)
    
  return(fmeasure)
} 




# saving space for the f measures that will be computed from the kmeans
kmeansArray.length <- length(kmeansArray)
fmeasureMatrix <- matrix(nrow = kmeansArray.length, ncol = 2)


























# iterating through all kmeans to calculate f measure 
for (kmeansIndex in 1:kmeansArray.length) {
  normalKmeans <- kmeansArray[[kmeansIndex]]
  irisSubSetWithClusterNames <- getIrisWithClusterNames(normalKmeans, irisSubSetWithSpecies)
  fmeasure <- getFmeasure(irisSubSetWithClusterNames)
  n <- toString(nrow(normalKmeans$centers))
  nKmeans <- paste("kmeans",n)
  fmeasureMatrix[kmeansIndex, 1] <- nKmeans
  fmeasureMatrix[kmeansIndex, 2] <- fmeasure


  postscript(paste(nKmeans, ".eps"))
  plot(normalKmeans$cluster)
  dev.off()
}




# printing results
print(fmeasureMatrix)