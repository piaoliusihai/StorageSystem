#!/bin/bash
#
# A script to test if the basic functions of the files
# in CloudFS. Has to be run from the src directory.
#

TEST_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

source $TEST_DIR/../../../scripts/paths.sh

REFERENCE_DIR="/tmp/cloudfstest"
LOG_DIR="/tmp/testrun-`date +"%Y-%m-%d-%H%M%S"`"
STAT_FILE="$LOG_DIR/stats"
THRESHOLD="16"
AVGSEGSIZE="4"
TEST_FILE="largefile"
MODIFIED_FILE="largefile.modified"
FILE_SIZE=$(wc -c < $TEST_DIR/$TEST_FILE)
CLOUD_USAGE="cloud_usage"

source $SCRIPTS_DIR/functions.sh
NODEDUP=0
#
# Execute battery of test cases.
# expects that the test files are in $FUSE_MNT_
# and the reference files are in $REFERENCE_DIR
# Creates the intermediate results in $LOG_DIR
#

process_args cloudfs --ssd-path $SSD_MNT_ --fuse-path $FUSE_MNT_ --threshold $THRESHOLD --avg-seg-size $AVGSEGSIZE

# test setup
rm -rf $REFERENCE_DIR
mkdir -p $REFERENCE_DIR
mkdir -p $LOG_DIR

reinit_env

#----
# Testcases assumes that test data does not have any hidden files(.* files)
# Students should have all their metadata in hidden files/dirs
echo ""
echo "Executing test_2_4"
echo -e "Running cloudfs in dedup mode\n"

#Copy the test file into the fuse folder and the reference folder
echo -e "Copying test file into the fuse folder..."
cp  $TEST_DIR/$TEST_FILE $FUSE_MNT/$TEST_FILE


collect_stats > $STAT_FILE
echo -e "\nCloud statistics -->"
echo "Capacity usage in cloud : $(get_cloud_max_usage $STAT_FILE)"
echo "Bytes read from cloud : $(get_cloud_read_bytes $STAT_FILE)"

# #Compare the number of bytes written to the cloud with the file size
# echo "$(get_cloud_current_usage $STAT_FILE)" > $LOG_DIR/$CLOUD_USAGE
# nbytes1=$(<$LOG_DIR/$CLOUD_USAGE)
# echo -ne "\nCheck if cloud usage is less than file size   " 
# test $nbytes1 -lt $FILE_SIZE 
# print_result $?

echo -ne "Read first 10 bytes   "
head -c 10 $FUSE_MNT/$TEST_FILE
collect_stats > $STAT_FILE
echo -e "\nCloud statistics -->"
echo "Capacity usage in cloud : $(get_cloud_max_usage $STAT_FILE)"
echo "Bytes read from cloud  : $(get_cloud_read_bytes $STAT_FILE)"

#----
#destructive test : always do this test at the end!!
echo -ne "\nFile removal test (rm -rf)        "
rm -rf $FUSE_MNT/*
LF="$LOG_DIR/files-remaining-after-rm-rf.out"

ls $FUSE_MNT_ > $LF
find $SSD_MNT_ \( ! -regex '.*/\..*' \) -type f >> $LF
find $S3_DIR \( ! -regex '.*/\..*' \) -type f >> $LF
nfiles=`wc -l $LF|cut -d" " -f1`
print_result $nfiles

# test cleanup
rm -rf $REFERENCE_DIR
rm -rf $LOG_DIR
exit 0
