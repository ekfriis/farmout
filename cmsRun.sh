#!/bin/sh
#

jobcfgs=$1
datafile=$2
SRM_OUTPUT_DIR=$3
if [ "$SRM_OUTPUT_DIR" = "." ]; then
  # no SRM stage-out; files are being copied back to submit dir
  SRM_OUTPUT_DIR=""
fi
if [ "$SRM_OUTPUT_DIR" != "" ]; then
  SRM_OUTPUT_FILE="$SRM_OUTPUT_DIR/$datafile"
  SRM_FAILED_OUTPUT_FILE="${SRM_OUTPUT_DIR}-cmsRun-failed/${datafile}"
fi

if [ "$OSG_GRID" != "" ]; then
  OSG_SETUP=$OSG_GRID/setup.sh
else
  OSG_SETUP=/afs/hep.wisc.edu/osg/wnclient/setup.sh
fi

# special exit status to force job to leave the queue
FAIL_JOB=42

SRM_TIMEOUT=3600

# core files have a nasty habit of filling up disks and causing trouble,
# so disable them.
ulimit -c 0

# load environment for using srmcp
source $OSG_SETUP


dashboard_completion() {
  export dboard_ExeExitCode=$1

  if [ "${FARMOUT_DASHBOARD_REPORTER}" != "" ]; then
      ${FARMOUT_DASHBOARD_REPORTER} completion
  fi
}

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
  if [ "$size" = "0" ]; then
    echo "Cleaning up zero-length destination file $srm_fname."
    srm-advisory-delete -debug=true -retry_num=0 "$srm_fname"
  fi
  return 1
}

RunWithTimeout() {
    timeout=$1
    shift

    "$@" &
    PID=$!

    start=`date "+%s"`
    soft_timeout_time=$(($start + $timeout))
    hard_timeout_time=$(($start + $timeout + 60))
    while [ 1 ]; do
        if ! kill -0 $PID >& /dev/null; then
            wait $PID
            return
        fi
        now=`date "+%s"`
        if [ $now -gt $hard_timeout_time ]; then
            echo "Hard killing pid $PID." 2>&1
            kill -9 $PID
        elif [ $now -gt $soft_timeout_time ]; then
            echo "Timed out after waiting $timeout seconds." 2>&1
            kill $PID
            soft_timeout_time=$hard_timeout_time
        fi
        sleep 1
    done
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

      RunWithTimeout $SRM_TIMEOUT srmcp -debug=true -retry_num=0 "$src" "$dest"
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

if [ "${FARMOUT_DASHBOARD_REPORTER}" != "" ]; then
    ${FARMOUT_DASHBOARD_REPORTER} submission
    ${FARMOUT_DASHBOARD_REPORTER} execution
fi

# create directory for intermediate output
mkdir intermediate

echo "Running on host `hostname`"
echo "Current working directory: `pwd`"
echo "df `pwd`"
df .

start_time=`date "+%s"`

cmsRun=cmsRun

for cfg in ${jobcfgs//,/ }; do
  if [ "$FWKLITE_SCRIPT" != "" ]; then
    cmsRun="./$FWKLITE_SCRIPT"
    chmod a+x $cmsRun
    if [ "${cmsRun/*./.}" = ".C" ]; then
      cmsRun="root -q -b $cmsRun"
    fi
    export INPUT="$cfg"
    export OUTPUT="$datafile"
    echo "farmout: starting $cmsRun with INPUT=$INPUT and OUTPUT=$OUTPUT at `date`"

    $cmsRun
    cmsRun_rc=$?

    echo "farmout: $cmsRun exited with status $cmsRun_rc at `date`"
  else
    echo "farmout: starting cmsRun $cfg at `date`"

    jobreport="${cfg%.*}.xml"
    cmsRun --jobreport=$jobreport $cfg
    cmsRun_rc=$?

    echo "farmout: cmsRun $cfg exited with status $cmsRun_rc at `date`"
  fi

  echo "ls -ltr . intermediate"
  ls -ltr . intermediate
  echo "End of ls output"

  if [ "$cmsRun_rc" != 0 ]; then
    break
  fi
done

export dboard_ExeTime=$((`date "+%s"` -  $start_time))

if [ "$cmsRun_rc" != "0" ]; then
  echo "$cmsRun exited with status $cmsRun_rc"
  if [ -f $datafile ] && [ "$SAVE_FAILED_DATAFILES" = "1" ] && [ "$SRM_OUTPUT_DIR" != "" ]; then
    if ! DoSrmcp "file://localhost/`pwd`/$datafile" "$SRM_FAILED_OUTPUT_FILE"; then
      echo "Failed to save datafile from failed run."
    fi
  fi
  rm -f $datafile
  rm -f *.root

  dashboard_completion $cmsRun_rc

  # Do not try to run this job again.  Would be nice to detect transient
  # errors (e.g. dCache down) and retry in those cases.
  exit $FAIL_JOB
fi


if ! [ -f $datafile ]; then
  echo "$cmsRun did not produce expected datafile $datafile"
  exit $FAIL_JOB
fi

if [ "$SRM_OUTPUT_DIR" != "" ]; then
  if ! DoSrmcp "file://localhost/`pwd`/$datafile" "$SRM_OUTPUT_FILE"; then
    dashboard_completion 60307
    rm -f *.root
    exit 1
  fi

  rm $datafile
fi

# Copy all other root files in the directory also

if [ "$SRM_OUTPUT_DIR" != "" ]; then
  for file in `ls -1 *.root 2>/dev/null`; do
      if ! DoSrmcp file://localhost/`pwd`/$file $SRM_OUTPUT_DIR/$file; then
          dashboard_completion 60307
          rm -f *.root
	  exit 1
      fi
      rm $file
  done
fi

dashboard_completion 0

exit 0
