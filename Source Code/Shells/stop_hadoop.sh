#!/bin/bash
echo " "
# Check which Hadoop processes are running
echo " "
echo " "
echo "Checking which processes are running right now..."
echo " "
jps


# Stop the HDFS daemons
echo " "
echo " "
echo "Stop the HDFS daemons..."
echo " "
stop-dfs.sh


# Stop the YARN daemons
echo " "
echo " "
echo "Stop the YARN daemons..."
echo " "
stop-yarn.sh


# Stop the Hadoop JobHistroyServer daemon
echo " "
echo " "
echo "Stop the Hadoop JobHistroyServer daemon..."
echo " "
mr-jobhistory-daemon.sh stop historyserver


# Check which Hadoop processes are running
echo " "
echo " "
echo "Checking Hadoop processes have stopped..."
echo " "
jps
