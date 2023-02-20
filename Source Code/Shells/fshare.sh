#!/bin/bash
echo " "
# Set up file share
echo " "
echo " "
echo "Set up file share..."

echo " "
echo "Read VM drive(root)..."
echo " "
ls -l

echo " "
echo "Set up share to external drive on Host..."
echo " "
sudo mount -t vboxsf Shared ~/shared

echo " "
echo "Read external shared drive..."
echo " "
ls ~/shared

echo " "
echo "Read VM drive(root)..."
echo " "
ls -l

