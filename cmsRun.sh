#!/bin/sh
#

start_time=`date "+%s"`
user_time=0
sys_time=0

jobcfgs=$1
shift
datafile=$1
shift
SRM_OUTPUT_DIR=$1
shift
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
elif [ -e /etc/osg/wn-client/setup.sh ]; then
  OSG_SETUP=/etc/osg/wn-client/setup.sh
else
  OSG_SETUP=/afs/hep.wisc.edu/osg/wnclient/setup.sh
fi

# special exit status to force job to leave the queue
FAIL_JOB=42

SRM_TIMEOUT=3600

# core files have a nasty habit of filling up disks and causing trouble,
# so disable them.
ulimit -c 0

# load environment for using lcg-cp
source $OSG_SETUP

dashboard_completion() {
  export dboard_ExeExitCode=$1

  if [ "${FARMOUT_DASHBOARD_REPORTER}" != "" ]; then
      ${FARMOUT_DASHBOARD_REPORTER} completion
  fi
}

outputFileExists() {
  local srm_fname="$1"

  local info="`lcg-ls -l -bD srmv2 $srm_fname 2>/dev/null`"
  if [ "$?" != "0" ]; then
    return 1
  fi
  local size="`echo \"$info\" | awk '{print $5}'`"
  if [ "$size" != "0" ] && [ "$size" != "" ]; then
    echo "$info"
    return 0
  fi
  if [ "$size" = "0" ]; then
    echo "Cleaning up zero-length destination file $srm_fname."
    lcg-del -l -bD srmv2 "$srm_fname"
  fi
  return 1
}

RunWithTimeout() {
    local timeout=$1
    shift

    "$@" &
    PID=$!

    local start=`date "+%s"`
    local soft_timeout_time=$(($start + $timeout))
    local hard_timeout_time=$(($start + $timeout + 60))
    while [ 1 ]; do
        if ! kill -0 $PID >& /dev/null; then
            wait $PID
            return
        fi
        local now=`date "+%s"`
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
    local src="$1"
    local dest="$2"

    local tries=0
    while [ "$tries" -lt 3 ]; do
      if [ "$tries" -gt 1 ]; then
        echo "Trying again at `date`: lcg-cp -bD srmv2 $src $dest"
      fi

      RunWithTimeout $SRM_TIMEOUT lcg-cp -bD srmv2 "$src" "$dest"
      rc=$?

      if [ "$rc" = "0" ]; then
        echo "successful file transfer: lcg-cp -bD srmv2 $src $dest"
        return 0
      fi

      echo
      echo
      echo "lcg-cp exited with non-zero status $rc at `date`."
      echo "This happened when copying $src to $dest."
      echo

      if outputFileExists "$dest"; then
         echo "Cleaning up failed destination file $dest."
         lcg-del -l -bD srmv2 "$dest"

         rc=$?
         if [ "$rc" != "0" ]; then
           echo "lcg-del failed with exit status $rc at `date`."
         fi
      fi

      tries=$(($tries+1))
    done

    echo "Giving up after $tries attempts to copy $src to $dest."
    return 1
}

exitSlowly() {
    local total_runtime=$((`date "+%s"` -  $start_time))
    if [ "$total_runtime" -lt 300 ]; then
        date
        echo "Waiting before exiting to avoid the possibility of rapid job failure on this node."
        sleep 300
        date
    fi
    exit "$1"
}

if outputFileExists $SRM_OUTPUT_FILE; then
  echo "File already exists: $SRM_OUTPUT_FILE; exiting as though successful."
  exit 0
fi

if [ "${CMS_DASHBOARD_REPORTER_TGZ}" != "" ]; then
    if ! tar xzf "${CMS_DASHBOARD_REPORTER_TGZ}"; then
        echo "WARNING: failed to untar ${CMS_DASHBOARD_REPORTER_TGZ}"
    else
        export CMS_DASHBOARD_REPORTER=$(basename "${CMS_DASHBOARD_REPORTER_TGZ}" .tgz)
    fi
fi

if [ "${DO_RUNTIME_CMSSW_SETUP}" = 1 ]; then
    # if HOME is not defined, root fails to expand $HOME/.root.mimes
    if [ "$HOME" = "" ]; then
        export HOME=`pwd`
    fi

    if ! [ -f "${VO_CMS_SW_DIR}/cmsset_default.sh" ]; then
        if [ -f "${OSG_APP}/cmssoft/cms/cmsset_default.sh" ]; then
            VO_CMS_SW_DIR="${OSG_APP}/cmssoft/cms/cmsset_default.sh"
        else
            echo "No such file ${VO_CMS_SW_DIR}/cmsset_default.sh"
            exitSlowly 1
        fi
    fi
    source "${VO_CMS_SW_DIR}/cmsset_default.sh"
    scram=scramv1
    if ! which $scram > /dev/null; then
        echo "Cannot find $scram in PATH"
        exitSlowly 1
    fi

    echo
    echo "Setting up ${CMSSW_VERSION}"

    if ! $scram project CMSSW "${CMSSW_VERSION}"; then
        echo "Failed to set up local project area for CMSSW_VERSION ${CMSSW_VERSION}"
        exitSlowly 1
    fi

    # copy in user analysis files
    if [ "${CMSSW_USER_CODE_TGZ}" != "" ]; then
        if ! tar xzf "${CMSSW_USER_CODE_TGZ}" -C "${CMSSW_VERSION}"; then
            echo "Failed to extract ${CMSSW_USER_CODE_TGZ}"
            exitSlowly 1
        fi
    fi

    # Condor (as of 7.9.1) privsep fails if there are symlinks to
    # private files or directories in the sandbox.  The job goes on
    # hold after completing.  The hold reason is "error changing
    # sandbox ownership to condor" See
    # https://condor-wiki.cs.wisc.edu/index.cgi/tktview?tn=2904

    find ${CMSSW_VERSION} -type d -exec chmod a+rx '{}' \;
    chmod -R a+r ${CMSSW_VERSION}

    cd ${CMSSW_VERSION}

    eval `$scram runtime -sh`
    cd ..
    ls -l -R ${CMSSW_VERSION}/bin ${CMSSW_VERSION}/lib

    echo "Done setting up ${CMSSW_VERSION}"
    echo

fi

# in case dashboard reporter is in cwd
export PATH="`pwd`:$PATH"

if [ "${FARMOUT_DASHBOARD_REPORTER}" != "" ]; then
    ${FARMOUT_DASHBOARD_REPORTER} submission
    ${FARMOUT_DASHBOARD_REPORTER} execution
fi

# create directory for intermediate output
mkdir intermediate


echo "Running on host `hostname`"
echo "uname: " `uname -a`
echo "/etc/issue: " `cat /etc/issue`
echo "Current working directory: `pwd`"
echo "df `pwd`"
df .

# in some environments, our stdout/stderr are not readable by others,
# which makes monitoring a pain when the file are transferred back
chmod a+r *.out *.err

if [ -n "${FARMOUT_HOOK_PRERUN}" ]; then
    if [ ! -x "${FARMOUT_HOOK_PRERUN}" ]; then
        echo "farmout: prerun hook ${FARMOUT_HOOK_PRERUN} is not executable"
    else
        echo "farmout: calling prerun hook ${FARMOUT_HOOK_PRERUN}"
        ${FARMOUT_HOOK_PRERUN}
        echo "farmout: prerun hook ${FARMOUT_HOOK_PRERUN} returned $?"
    fi
fi

# set vsize limit here, after file checks, so this does not
# interfere with grid tools
echo
if [ "$FARMOUT_VSIZE_LIMIT" != "" ]; then
  echo "Setting virtual memory limit to $FARMOUT_VSIZE_LIMIT MB."
  ulimit -S -v $(( $FARMOUT_VSIZE_LIMIT * 1024 )) 2>&1
fi

# We have observed problems with cmsRun using abnormally large amounts
# of memory under Condor 7.7.0 due to the default stack size being
# 500 MB rather than 10 MB
FARMOUT_STACK_SIZE=${FARMOUT_STACK_SIZE:-10}
if [ "${FARMOUT_STACK_SIZE}" != "default" ]; then
  echo "Setting stack limit to $FARMOUT_STACK_SIZE MB."
  ulimit -S -s $(( $FARMOUT_STACK_SIZE * 1024 )) 2>&1
fi

ulimit -a
echo

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

    /usr/bin/time -p -o exe_time $cmsRun "$@"
    cmsRun_rc=$?

    echo "farmout: $cmsRun exited with status $cmsRun_rc at `date`"
  else
    echo "farmout: starting cmsRun $cfg at `date`"

    jobreport="${cfg%.*}.xml"
    /usr/bin/time -p -o exe_time cmsRun --jobreport=$jobreport $cfg "$@"
    cmsRun_rc=$?

    echo "farmout: cmsRun $cfg exited with status $cmsRun_rc at `date`"
    if [ "$cmsRun_rc" = 255 ] && [ "$FARMOUT_VSIZE_LIMIT" != "" ]; then
      echo
      echo "farmout: WARNING: This may be an indication that cmsRun hit the vsize limit of "$FARMOUT_VSIZE_LIMIT" MB"
      echo
    fi
  fi

  user_time=`awk "/^user/ {print ${user_time}+\\$2}" exe_time`
  sys_time=`awk "/^sys/ {print ${sys_time}+\\$2}" exe_time`

  echo "time output:"
  cat exe_time

  echo "ls -ltr . intermediate"
  ls -ltr . intermediate
  echo "End of ls output"

  if [ "$cmsRun_rc" != 0 ]; then
    break
  fi
done

export dboard_ExeTime=$((`date "+%s"` -  $start_time))

export dboard_ExeCPU=$(echo | awk "{print $user_time + $sys_time}")

# Technically, the following should include cpu time for cmsRun + cmsRun.sh
# and everything that happens in the job.  However, we just report the
# exe cpu time.
export dboard_CrabUserCpuTime=$user_time
export dboard_CrabSysCpuTime=$sys_time

# remove vsize limit so it does not interfere with file transfer
ulimit -S -v unlimited 2>&1

# get rid of CMSSW environment, because it can cause incompatibilities
# with libraries required by lcg-cp
eval `scram runtime unsetenv -sh`

if [ "$cmsRun_rc" != "0" ]; then
  echo "$cmsRun exited with status $cmsRun_rc"
  if [ -f $datafile ] && [ "$SAVE_FAILED_DATAFILES" = "1" ] && [ "$SRM_OUTPUT_DIR" != "" ]; then
    if ! DoSrmcp "$datafile" "$SRM_FAILED_OUTPUT_FILE"; then
      echo "Failed to save datafile from failed run."
    fi
  fi
  rm -f $datafile
  rm -f *.root

  dashboard_completion $cmsRun_rc

  if [ "$cmsRun_rc" = "65" ]; then
    # We have seen this error caused by timeouts in connecting to frontier,
    # so try the job again.
    exitSlowly 1
  elif ! touch intermediate/touch_test; then
    # The local disk appears to be messed up, so try the job again.
    echo "Failed to touch a file in the sandbox."
    exitSlowly 1
  else
    # Do not try to run this job again.
    exitSlowly $FAIL_JOB
  fi
fi

if [ -n "${FARMOUT_HOOK_POSTRUN}" ]; then
    if [ ! -x "${FARMOUT_HOOK_POSTRUN}" ]; then
        echo "farmout: postrun hook ${FARMOUT_HOOK_POSTRUN} is not executable"
    else
        echo "farmout: calling postrun hook ${FARMOUT_HOOK_POSTRUN}"
        ${FARMOUT_HOOK_POSTRUN}
        echo "farmout: postrun hook ${FARMOUT_HOOK_PRERUN} returned $?"
    fi
fi

if [ "$JOB_GENERATES_OUTPUT_NAME" != 1 ]; then
  if ! [ -f $datafile ]; then
    echo "$cmsRun did not produce expected datafile $datafile"
    exitSlowly $FAIL_JOB
  fi

  if [ "$SRM_OUTPUT_DIR" != "" ]; then
    if ! DoSrmcp "$datafile" "$SRM_OUTPUT_FILE"; then
      dashboard_completion 60307
      rm -f *.root
      exitSlowly 1
    fi

    rm $datafile
  fi
fi

# Copy all other root files in the directory also

if [ "$SRM_OUTPUT_DIR" != "" ]; then
  for file in `ls -1 *.root 2>/dev/null`; do
      if ! DoSrmcp $file $SRM_OUTPUT_DIR/$file; then
          dashboard_completion 60307
          rm -f *.root
	  exitSlowly 1
      fi
      rm $file
  done
fi

dashboard_completion 0

exit 0
