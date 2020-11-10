# INC-SC-DBSCAN: Incremental Schema Discovery for RDF Data at Scale
INC-SC-DBSCAN is an incremental schema discovery approach for massive RDF data. It is based on a scalable and incremental density-based clustering algorithm which propagates the changes occurring in the dataset into the clusters corresponding to the classes of the schema.

INC-SC-DBSCAN is implemented in [Scala](https://www.scala-lang.org/) and using the [Apache Spark](https://spark.apache.org/) framework.

## Building the project
[Maven](https://maven.apache.org/) is used to build the project.
The [Maven wrapper](https://github.com/takari/maven-wrapper) tool allows to build the project without a local maven install.
Due to some constraints imposed by the Scala compiler, a [JDK 8](https://adoptopenjdk.net/) is needed.

On Linux
```
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
./mvnw package -DskipTests
```

On Windows
```
set JAVA_HOME=C:\path\to\jdk8
mvnw.cmd package -DskipTests
```

## Running the algorithm
The main class is `david/sc_dbscan/Main.scala`.

```
spark-submit --class david.sc_dbscan.Main \\
             target/sc_dbscan-1.0-jar-with-dependencies.jar \\
             --eps X.X --coef Y --cap C  --mpts Y --oldData F  dataset

WHERE:
  --eps 	: the similarity threshold epsilon (between 0 and 1)
  --coef 	: a boolean that defines whether it clusters patterns or entities
  --cap 	: the maximum capacity of a computing node (in number of entities)
  --mpts 	: the density thresholg minPts
  --oldData : the clustering result of previous processes
  dataset   : the path to the dataset
```

For Example
```
spark-submit --class david.sc_dbscan.Main \\
             target/sc_dbscan-1.0-jar-with-dependencies.jar \\
             --eps 0.8 --coef false --cap 2000  --mpts 3 --oldData DataSets/T1000L10N10000_clusters DataSets/T100L10N10000
