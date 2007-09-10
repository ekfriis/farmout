#!/bin/sh
#

jobcfg=$1
datafile=$2
SRM_OUTPUT_DIR=$3
SRM_OUTPUT_FILE="$SRM_OUTPUT_DIR/$datafile"
OSG_SETUP=/afs/hep.wisc.edu/cms/sw/osg0.4.1/setup.sh

# core files have a nasty habit of filling up disks and causing trouble,
# so disable them.
ulimit -c 0

outputFileExists() {
  srm_fname="$1"

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
  unset IFS
  if [ "$size" != "0" ] && [ "$size" != "" ]; then
    return 0
  fi
  return 1
}

DoSrmcp() {
    src="$1"
    dest="$2"

    # The internal retries in srmcp are often useless, because a broken file
    # may be left behind (as of dCache 1.7.0-38), so further attempts to
    # copy the file will all fail with the error "file exists".  Therefore,
    # we have our own retry loop, including deletion of broken files.

    tries=0
    while [ "$tries" -lt 3 ]; do
      if [ "$tries" -gt 1 ]; then
        echo "Trying again at `date`: srmcp $src $dest"
      fi

      srmcp -debug=true -retry_num=0 "$src" "$dest"
      rc=$?

      if [ "$rc" = "0" ]; then
        return 0
      fi

      echo
      echo
      echo "srmcp exited with non-zero status $rc at `date`."
      echo "This happened when copying $src to $dest."
      echo

      if outputFileExists "$dest"; then
         echo "Cleaning up failed destination file $dest."
         srm-advisory-delete -debug=true -retry_num=0 "$dest"

         rc=$?
         if [ "$rc" != "0" ]; then
           echo "srm-advisory-delete failed with exit status $rc at `date`."
         fi
      fi

      tries=$(($tries+1))
    done

    echo "Giving up after $tries attempts to copy $src to $dest."
    return 1
}


if outputFileExists $SRM_OUTPUT_FILE; then
  echo "File already exists: $SRM_OUTPUT_FILE; exiting as though successful."
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

# load environment for using srmcp
source $OSG_SETUP

if ! DoSrmcp "file://localhost/`pwd`/$datafile" "$SRM_OUTPUT_FILE"; then
  exit 1
fi

rm $datafile

# Copy all other root files in the directory also

for file in `ls -1 *.root 2>/dev/null`; do
    if ! DoSrmcp file://localhost/`pwd`/$file $SRM_OUTPUT_DIR/$file; then
	exit 1
    fi
    rm $file
done

exit 0
