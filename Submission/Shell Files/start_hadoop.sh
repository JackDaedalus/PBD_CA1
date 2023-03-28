#!/bin/bash
echo " "
echo "Running start up script for Hadoop..."

# Check Version
echo " "
echo " "
echo "Check Hadoop version..."
echo " "
hadoop version

# Verify if any the Hadoop services are running
echo " "
echo " "
echo "Verify if any the Hadoop services are running..."
echo " "
jps

# Start the Hadoop HDFS services
echo " "
echo " "
echo "Start the Hadoop HDFS services..."
echo " "
start-dfs.sh

# Start the YARN daemons
echo " "
echo " "
echo "Start the YARN daemons..."
echo " "
start-yarn.sh


# Start the Hadoop JobHistroyServer daemon
echo " "
echo " "
echo "Start the Hadoop JobHistroyServer daemon..."
echo " "
mr-jobhistory-daemon.sh start historyserver


# Verify that the Hadoop services are running
echo " "
echo " "
echo "Verify that the Hadoop services are running..."
echo " "
jps
