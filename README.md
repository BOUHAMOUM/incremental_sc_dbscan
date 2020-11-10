# INC-SC-DBSCAN: Incremental Schema Discovery for RDF Data at Scale
INC-SC-DBSCAN is an incremental schema discovery approach for massive RDF data. It is based on a scalable and incremental density-based clustering algorithm which propagates the changes occurring in the dataset into the clusters corresponding to the classes of the schema.

## Compilation
Ce projet est compilable avec maven dans un environnement Java 8.
```bash
export JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64/"
mvn package
```

## Génération d'un jar incluant les dépendances
```bash
mvn clean package assembly:single
```
