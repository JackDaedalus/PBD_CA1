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
hadoop fs -put Il_Diavolo_Italian gutenbergBooks
hadoop fs -put Die_Postgeheimnisse_German gutenbergBooks
hadoop fs -put DivinaCommedia_Italian gutenbergBooks
hadoop fs -put Don_Quijote_Spanish gutenbergBooks
hadoop fs -put Goethe_Biographie_German gutenbergBooks
hadoop fs -put La_isla_del_tesoro_Spanish gutenbergBooks

