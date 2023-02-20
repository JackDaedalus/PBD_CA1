#!/bin/bash
echo " "
# Set up file copy
echo " "
echo " "
echo "Set up file copy of Gutenberg books..."
echo " "

echo "List content of external drive on Host..."
echo " "
ls ~/shared -l

echo " "
echo "Copy Gutenberg book file from Host..."
echo " "
sudo cp ~/shared/Il_Diavolo.txt Il_Diavolo
echo " "

echo " "
echo "Check file directoty on VM..."
echo " "
ls -l

echo " "
echo "Set up file permission on VM..."
echo " "
sudo chown soc:soc Il_Diavolo


echo " "
echo " "
echo "Re-check file directoty on VM..."
echo " "
ls -l
