#!/bin/bash
echo " "
# Make new directory on HDFS
echo " "
echo " "
echo "Load book data into HDFS..."
hadoop fs -mkdir gutenbergBooks

# Load data into HDFS
echo " "
echo " "
echo "Load book data into HDFS..."
hadoop fs -put Il_Diavolo gutenbergBooks
