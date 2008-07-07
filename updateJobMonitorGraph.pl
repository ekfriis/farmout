#! /usr/bin/perl
#
# Reads from a data file, 
# creates a gif plot, and 
# makes a webpage to view it.
#
# Michael Anderson, July 5 2008

##################################################
# SECTION 1:
# Set variables for the locations and 
# names of all the files.
$user = $ENV{'USER'};
$homeDir = "/afs/hep.wisc.edu/home/$user/";
$logfile = "$homeDir/jobMonitor.log";

# The webpage and gif file will be placed in webDir
$webDir = "$homeDir/www/";
$webPage = "jobMonitor.php";
$giffile = "jobMonitor.gif";
$webSiteAddr = "http://www.hep.wisc.edu/~$user/$webPage";

# Temp files and executable locations
$datafile = "/tmp/gnuplot_$$.dat";
$gpfile = "/tmp/gnuplot_$$.gp";
$GNUPLOT = "gnuplot";
$PPMTOGIF = "ppmtogif";
$farmoutSumLog = "/tmp/farmoutSummary.log";
##################################################


##################################################
# SECTION 2:
# Get today's date
$datestr = `date`;
# remove the newline character
chop $datestr;
# put the day of week, month, etc, into variables
($dow, $mon, $day, $hour, 
 $min, $sec, $zone, $year) = split (/[ ]+|:/, $datestr);
# set date to day month year
$date = sprintf ("%02d/%s/%d", $day, $mon, $year);
##################################################


##################################################
# SECTION 3:
# Read log file with format:
# hour min totalJobs idleJobs runningJobs heldJobs
# example:
# 15 36 100 36 64 0
open (LOGFILE, "< $logfile");
while ($hi = <LOGFILE>) {

  # Read this line
  ($inhour, $inmin, $totalJobs, $jobsIdle, $jobsRunning, $jobsHeld) = split (" ", $hi);
  $hr_percent = ($inmin/60) * 100;
  $index = int($inhour * 100 + $hr_percent);
  # Read in from file and set the variables
  $totalJobsArray[$index]   = $totalJobs;
  $jobsIdleArray[$index]    = $jobsIdle;
  $jobsRunningArray[$index] = $jobsRunning;
  $jobsHeldArray[$index]    = $jobsHeld;

  # Count the total number of lines
  $total++;
}
close LOGFILE;
##################################################


##################################################
# SECTION 4:
# Write the data file that gnuplot will use. 
open (DATAFILE, "> $datafile");
printf (DATAFILE "#HourMin\tTotalJobs\tIdleJobs\tRunningJobs\tHeldJobs\n");
for ($i=0; $i <= 2400; $i++) {
    if (defined ($totalJobsArray[$i])) {
	printf (DATAFILE "%04d\t%d\t%d\t%d\t%d\n", $i, $totalJobsArray[$i], $jobsIdleArray[$i], $jobsRunningArray[$i], $jobsHeldArray[$i]);
    } else {
	printf (DATAFILE "%04d\t0\t0\t0\t0\n", $i);
    }
}
close DATAFILE;
##################################################


##################################################
# SECTION 5:
# Write the gnuplot command file.
# Define custom tic marks for the x axis.
for ($i=0; $i <=24; $i++) {
  if ($i == 12) {
    $xtics = sprintf ("%s\"%s\" %d,", 
		       $xtics, "Noon", $i*100);
  } else {
    $xtics = sprintf ("%s\"%02d\" %d,", 
		       $xtics, $i, $i*100);
  }
}
chop $xtics;
open (GPFILE, "> $gpfile");

# Everything after the following line up to the
# terminating "EOM" line defines the contents of
# the gnuplot command file.
print GPFILE <<EOM;
set term pbm color
set offsets
set nolog
set nopolar
set border
set grid
set title "Jobs on $mon $day, $year (updated $hour:$min)"
set xlabel "Time (Hours)"
set ylabel "Number of Jobs"
set size 1.20, 0.50
set xtics ($xtics)
set key outside
# Main plot command
plot "$datafile" using 1:2 title 'Total Jobs' with points,  "$datafile" using 1:3 title 'Idle' with points, "$datafile" using 1:4 title 'Running' with lines, "$datafile" using 1:5 title 'Held' with points
EOM
close GPFILE;

##################################################
# SECTION 6:
# Run gnuplot, and convert the output to gif
# format.
system ("$GNUPLOT $gpfile | $PPMTOGIF 2> /dev/null > $webDir/$giffile");
#Now delete the gnuplot command and data file
#unlink ($gpfile, $datafile);
##################################################


##################################################
# SECTION 7:
# Output an HTML page to display the graphic we
# just generated.

#Top half of web page
open (WEBPAGE, "> $webDir/$webPage");
print WEBPAGE <<EOM;
<HTML>
<head>
 <title>Job Monitoring</title>
 <LINK REL="SHORTCUT ICON" HREF="http://www.hep.wisc.edu/cms/comp/cmsIcon.ico">
</head>
<BODY>
<CENTER><IMG SRC=$giffile></CENTER>
<hr>
<h2>Info on jobs submitted by farmoutAnalysisJobs:</h2>
<pre>
EOM
close WEBPAGE;

# Farmout Summary information
system ("cat $farmoutSumLog >> $webDir/$webPage");

# Bottom half of web page
open (WEBPAGE, ">> $webDir/$webPage");
print WEBPAGE <<EOM;
</pre>
<hr>
<h2>Command cheat-sheet:</h2>
<?php include("/cms/www/comp/commands.php"); ?>
<hr>
<a href="http://www.hep.wisc.edu/cms/comp/userdoc.html">Wisconsin CMS User Documentation</a>
</BODY>
</HTML>
EOM
close WEBPAGE;
##################################################
