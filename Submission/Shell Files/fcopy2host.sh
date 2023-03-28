#!/bin/bash
echo " "
# Set up file copy to Host
echo " "
echo " "
echo "Set up file copy to Host..."
echo " "

echo "List content of external drive on Host before copy..."
echo " "
ls ~/shared -l

echo " "
echo "Copy file 1 to Host..."
echo " "
sudo cp dsLanguageFreqOne.txt ~/shared/ouput/dsLanguageFreqOne.txt
echo " "

echo " "
echo "Copy file 2 to Host..."
echo " "
# sudo cp hadoop-mapreduce-client-core-2.2.0.jar ~/shared/hadoop-mapreduce-client-core-2.2.0.jar
echo " "

echo "List content of external drive on Host after copy..."
echo " "
ls ~/shared -l
