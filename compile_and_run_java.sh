#!/bin/bash

# Define the directory containing Jena JAR files
JENA_LIB_DIR="apache-jena-fuseki-5.0.0"

# Include all JAR files in the Jena lib directory
CLASSPATH="$JENA_LIB_DIR/*:."

# Compile the Java program
javac -cp $CLASSPATH SPARQLQueryExecutor.java

# Check if compilation was successful
if [ $? -eq 0 ]; then
    echo "Compilation successful"
else
    echo "Compilation failed"
    exit 1
fi
