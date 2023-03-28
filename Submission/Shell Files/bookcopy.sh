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
sudo cp ~/shared/Il_Diavolo.txt 			Il_Diavolo_Italian
sudo cp ~/shared/Die_Postgeheimnisse.txt 	Die_Postgeheimnisse_German
sudo cp ~/shared/DivinaCommedia.txt 		DivinaCommedia_Italian
sudo cp ~/shared/Don_Quijote.txt 			Don_Quijote_Spanish
sudo cp ~/shared/Goethe_Biographie.txt 		Goethe_Biographie_German
sudo cp ~/shared/La_isla_del_tesoro.txt 	La_isla_del_tesoro_Spanish
echo " "

echo " "
echo "Check file directoty on VM..."
echo " "
ls -l

echo " "
echo "Set up file permission on VM..."
echo " "
sudo chown soc:soc Il_Diavolo_Italian
sudo chown soc:soc Die_Postgeheimnisse_German
sudo chown soc:soc DivinaCommedia_Italian
sudo chown soc:soc Don_Quijote_Spanish
sudo chown soc:soc Goethe_Biographie_German
sudo chown soc:soc La_isla_del_tesoro_Spanish

echo " "
echo " "
echo "Re-check file directoty on VM..."
echo " "
ls -l
