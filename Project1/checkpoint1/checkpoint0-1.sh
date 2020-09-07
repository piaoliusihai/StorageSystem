#!/bin/bash

myhostname=$(hostname -d)
myusername=$(whoami)
ami_id=$(curl http://169.254.169.254/latest/meta-data/ami-id 2>/dev/null)

echo ${myhostname} > checkpoint0.log
echo ${myusername} >> checkpoint0.log
echo ${ami_id} >> checkpoint0.log

if [[ "$myhostname" != "ec2.internal" ]]; then
    echo "Are you running on AWS? Current hostname is ${myhostname}"
    exit
fi

if [[ "$myusername" != "student" ]]; then
    echo "Are you running on AWS? Current username is ${myusername}"
    exit
fi

if [[ "$ami_id" != "ami-0083fabc7f3d8716a" ]]; then
    echo "Are you running on AWS? Current ami_id is ${ami_id}"
    exit
fi

echo "Great, it looks like you are using AWS."
read -r -p "Have you requested an AWS voucher [y/n]? " response
response=${response,,}  # to lower

if [[ "$response" = "y" ]]; then
    read -p "Please enter your voucher code: " answer
    echo ${answer: (-4)} >> checkpoint0.log

elif [[ "$response" = "n" ]]; then
     read -r -p "Do you need a voucher? [y/n]? " needVoucher
     needVoucher=${needVoucher,,}  # to lower
     if [[ "$needVoucher" = "y" ]]; then
        echo "You need to obtain your voucher before completing checkpoint 0."
        echo "Follow the instructions on the handout and FAQ page."
        exit
     elif [[ "$needVoucher" = "n" ]]; then
          echo "no" >> checkpoint0.log
     else
         echo "Invalid response."
         exit
     fi

else
    echo "Invalid response."
    exit
fi

echo "Checkpoint 0 complete! Please submit checkpoint0.log to Autolab."
