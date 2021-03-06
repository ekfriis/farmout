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
$homeDir = "/afs/hep.wisc.edu/home/$user";
$logfile = "$homeDir/jobMonitor.log";

# The webpage and gif file will be placed in webDir
$webDir = "$homeDir/public_html";
$webPage = "jobMonitor.php";
$imgFile = "jobMonitor.png";

# GNUPLOT Setup
$GNUPLOT = "gnuplot";
# Locations for temporary files (wherever you want)
$gpfile = "/tmp/gnuplot_$$.gp";
$farmoutSumLog = "/tmp/$user-farmoutSummary.log";
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
# Write the gnuplot command file
open (GPFILE, "> $gpfile");
# Everything after the following line up to the
# terminating "EOM" line defines the contents of
# the gnuplot command file.
print GPFILE <<EOM;
set terminal png transparent nocrop enhanced size 620,280
set output '$webDir/$imgFile'
set grid
set title "$user\'s Jobs ($mon $day, $year)"
set xlabel "Time (Hours)"
set ylabel "Number of Jobs"
set key reverse Left outside
set key autotitle columnheader
set xdata time
set timefmt "%m-%d-%H:%M"
set format x "%H:%M"
set style fill solid 1.00 noborder
# Main plot command
# This will stack the data
plot "$logfile" using 1:(\$3+\$4+\$5) title 3 with boxes, '' using 1:(\$4+\$5) title 4 with boxes, '' using 1:5 title 5 with boxes
EOM
close GPFILE;
##################################################


##################################################
# SECTION 4:
# Run gnuplot with the above created command file
system ("$GNUPLOT $gpfile");
# Now delete the gnuplot command and data file
unlink ($gpfile, $datafile);
##################################################


##################################################
# Optional Section
#  Check user's disk usage and give notice
#  if they're using a lot.
$diskUseString="";
$diskUseFile='/cms/www/comp/cmsprod/dCacheUserUsage.txt';
if (-r $diskUseFile) {
  $diskUseRank=`grep $user $diskUseFile | awk '{print \$3}'`;
  if ($diskUseRank > 5000) {
    $diskUseString="<a href=\"http://www.hep.wisc.edu/cms/comp/cmsprod/dCacheUserUsage.html\">$user\'s disk use</a> is greater than 5000 GB. Please consider <br>removing unused files. (Disk use is auto-checked only once a day).";
  }
}
##################################################


##################################################
# SECTION 5:
# Output an HTML page to display the graphic we
# just generated.

#Top half of web page
open (WEBPAGE, "> $webDir/$webPage");
print WEBPAGE <<EOM;
<HTML>
<META HTTP-EQUIV="REFRESH" CONTENT="120">
<HEAD>
 <title>Job Monitoring</title>
 <LINK REL="SHORTCUT ICON" HREF="http://www.hep.wisc.edu/cms/comp/cmsIcon.ico">
</HEAD>
<BODY>
<CENTER><IMG SRC=$imgFile><br>
Updated $mon $day, $hour:$min</CENTER>
$diskUseString
<hr>
<pre>
EOM
close WEBPAGE;

# Farmout Summary information
system ("farmoutSummary > $farmoutSumLog");
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
