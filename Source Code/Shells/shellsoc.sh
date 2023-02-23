#!/bin/bash
echo " "

echo " "
echo "Check file directoty on VM..."
echo " "
ls -l

echo " "
echo "Set up file permission on VM..."
echo " "
sudo chown soc:soc bookcopy.sh
sudo chown soc:soc deloutput.sh
sudo chown soc:soc filecopy.sh
sudo chown soc:soc fshare.sh
sudo chown soc:soc runjar.sh
sudo chown soc:soc loadbooks2HDFS.sh