#!/bin/bash
echo " "
# Delete MyOuput directory
echo " "

echo "Show myOutput directory before delete..."
echo " "
hadoop fs -ls

echo " "
echo "Delete MyOutput directory..."
echo " "
hdfs dfs -rm -r myOutput2

echo " "
echo "Delete Gutenberg Books directory..."
echo " "
# hdfs dfs -rm -r gutenbergBooks

echo " "
echo "Delete CA1 Output directory..."
echo " "
hdfs dfs -rm -r CA1_Output

echo " "
echo "Delete CA1 Output directory..."
echo " "
hdfs dfs -rm -r CA1_Output_out

echo "Show myOutput directory AFTER delete..."
echo " "
hadoop fs -ls
