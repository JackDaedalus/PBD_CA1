#!/bin/bash
echo " "
# Set up file copy
echo " "
echo " "
echo "Set up file copy..."
echo " "

echo "List content of external drive on Host..."
echo " "
ls ~/shared -l

echo " "
echo "Copy file from Host..."
echo " "
sudo cp ~/shared/AverageLetterFrequency.jar AverageLetterFrequency.jar
echo " "

echo " "
echo "Check file directoty on VM..."
echo " "
ls -l

echo " "
echo "Set up file permission on VM..."
echo " "
sudo chown soc:soc AverageLetterFrequency.jar


echo " "
echo " "
echo "Re-check file directoty on VM..."
echo " "
ls -l
