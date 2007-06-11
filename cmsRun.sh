#!/bin/sh
#

jobcfg=$1
datafile=$2
SRM_OUTPUT_DIR=$3
SRM_OUTPUT_FILE=$SRM_OUTPUT_DIR/$datafile
OSG_SETUP=/afs/hep.wisc.edu/cms/sw/osg0.4.1/setup.sh

# core files have a nasty habit of filling up disks and causing trouble
ulimit -c 0

outputFileExists() {
  srm_fname=$1
  source $OSG_SETUP
  info="`srm-get-metadata -retry_num=0 $srm_fname 2>/dev/null`"
  if [ "$?" != "0" ]; then
    return 1
  fi
  size_line="`echo \"$info\" | grep \"size :\"`"
  if [ "$?" != "0" ]; then
    return 1
  fi
  IFS=$' :\t'
  size="`echo \"$size_line\" | ( read label size; echo $size )`"
  if [ "$size" != "0" ] && [ "$size" != "" ]; then
    return 0
  fi
  return 1
}

if outputFileExists $SRM_OUTPUT_FILE; then
  echo "File already exists: $SRM_OUTPUT_FILE; aborting."
  exit 0
fi

cmsRun $jobcfg

rc=$?
if [ "$rc" != "0" ]; then
  echo "cmsRun exited with status $rc"
  if [ -f $datafile ]; then
    mv $datafile cmsRun_failed_$datafile
  fi
  exit 1
fi
source $OSG_SETUP
srmcp -debug=true file://localhost/`pwd`/$datafile $SRM_OUTPUT_FILE

rc=$?
if [ "$rc" != "0" ]; then
  echo "srmcp exited with status $rc"
  exit 1
fi
rm $datafile

# Copy all other root files in the directory also

for file in `ls -1 *.root 2>/dev/null`; do
    srmcp -debug=true file://localhost/`pwd`/$file $SRM_OUTPUT_DIR/$file
    if [ "$rc" != "0"]; then
	echo "srmcp exited with status $rc for $file"
	exit 1
    fi
    rm $file
done

exit 0
