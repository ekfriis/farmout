#!/usr/bin/env python

import logging
import os
import re
import string
import sys
import time
import xml.parsers.expat

__all__ = [
	"Error",
	"ParseError",
	"CondorUserLogEvent",
	"CondorUserLog",
	"CondorUserLogStats",
]

try:
	NullHandler = logging.NullHandler
except AttributeError:
	class NullHandler(logging.Handler):
		def emit(self, record): pass
log = logging.getLogger(__name__)
log.addHandler(NullHandler())

class Error(Exception): pass
class ParseError(Error): pass

class MyXMLElement(object):
	name = None
	attrs = None
	data = None
	elements = None

	def __init__(self):
		self.attrs = {}
		self.elements = []

class MyXMLParser(object):
	elements = None
	stack = None
	current = None

	def __init__(self):
		self.stack = []
		self.elements = []

	def ParseFile(self,fname):
		xml = file(fname,"r").read()
		return self.Parse(xml)

	def Parse(self,txt):
		p = xml.parsers.expat.ParserCreate()
		p.StartElementHandler = self.start_element
		p.EndElementHandler = self.end_element
		p.CharacterDataHandler = self.character_data
		p.Parse(txt)
		return self.elements

	def start_element(self,name,attrs):
		if self.current:
			self.stack.append(self.current)
			self.current = None

		self.current = MyXMLElement()
		self.current.name = name
		self.current.attrs = attrs

	def end_element(self,name):
		e = self.current
		if e.name <> name:
			raise ParseError("Mismatched XML end element: " + name + " vs. " + e.name)

		self.current = None
		if self.stack:
			self.current = self.stack.pop()
			self.current.elements.append(e)
		else:
			self.elements.append(e)

	def character_data(self,data):
		self.current.data = data


def unquoteClassAdString(s):
	return s.strip('"')


SUBMIT_EVENT = 0
EXECUTING_EVENT = 1
ABORTED_EVENT = 9
EVICTED_EVENT = 4
TERMINATED_EVENT = 5
JOB_AD_INFORMATION_EVENT = 28
RECONNECT_FAILED_EVENT = 24

class CondorUserLogEvent(object):
	event_type = None
	job_id = None
	timestamp = None
	event_str = None
	ExitCode = None
	ExitSignal = None
	RemoteHost = None
	MachineAttrGLIDEIN_Site0 = None
	MachineAttrName0 = None
	ImageSize = None

	def __str__(self):
		s = "type " + str(self.event_type)
		s += "\n  JobID " + str(self.job_id)
		if self.ExitCode != None:
			s += "\n  ExitCode " + str(self.ExitCode)
		if self.ExitSignal != None:
			s += "\n  ExitSignal " + str(self.ExitSignal)
		if self.RemoteHost:
			s += "\n  RemoteHost " + str(self.RemoteHost)
		if self.MachineAttrGLIDEIN_Site0:
			s += "\n  GLIDEIN_Site0 " + str(self.MachineAttrGLIDEIN_Site0)
		if self.MachineAttrName0:
			s += "\n  SlotName " + str(self.MachineAttrName0)
		if self.ImageSize:
			s += "\n  ImageSize " + str(self.ImageSize)
		s += "\n"
		return s

class CondorUserLog(object):
	events = None
	fname = None

	def __init__(self):
		self.events = []

	def __str__(self):
		return "".join(str(event) for event in self.events)

	def HasPendingJobs(self):
		#Returns true if any jobs are still submitted
		#(could be idle/running/held)

		pending_jobs = []
		for event in self.events:
			if event.event_type == SUBMIT_EVENT:
				pending_jobs.append(event.job_id)
			if event.event_type == ABORTED_EVENT or \
			   event.event_type == TERMINATED_EVENT:
				try:
					pending_jobs.remove(event.job_id)
				except ValueError:
					#Unfortunately, the following happens regularly when
					#the routed job is removed, because this gets logged
					#in the original ulog.
					#sys.stderr.write("WARNING: unexpected completion event for '" + event.job_id + "' in user log '" + self.fname + "'\n")
					pass

		return bool(pending_jobs)

	def JobFailed(self):
		#Returns true if all jobs exited with non-zero status or were removed

		pending_jobs = []
		success_jobs = []
		failed_jobs = []
		for event in self.events:
			if event.event_type == SUBMIT_EVENT:
				pending_jobs.append(event.job_id)
			if event.event_type == ABORTED_EVENT:
				try:
					pending_jobs.remove(event.job_id)
				except ValueError:
					pass
				failed_jobs.append(event.job_id)
			if event.event_type == TERMINATED_EVENT:
				try:
					pending_jobs.remove(event.job_id)
				except ValueError:
					pass
				if event.ExitSignal is None and event.ExitCode == 0:
					success_jobs.append(event.job_id)
				else:
					failed_jobs.append(event.job_id)

		return len(pending_jobs)==0 and len(success_jobs)==0 and len(failed_jobs)>0

	def ReadCondorUserLogXML(self,fname):
		p = MyXMLParser()
		xml = file(fname,"r").read()
		xml = "<userlog>" + xml + "</userlog>"
		try:
			elements = p.Parse(xml)
		except:
			sys.stderr.write("Failed to parse " + str(fname) + "\n")
			raise
		for c in elements[0].elements:
			event = CondorUserLogEvent()
			self.events.append(event)
			for a in c.elements:
				name = a.attrs["n"]
				value = a.elements[0].data
				if value == None and a.elements[0].attrs.has_key("v"):
					value = a.elements[0].attrs["v"]
				log.debug("name=%s, value=%s", name, value)
				if name == "EventTypeNumber":
					event.event_type = long(value)
				elif name == "Cluster":
					event.job_id = value
				elif name == "Proc":
					event.job_id += "." + value
				else:
					if event.event_str == None:
						event.event_str = ""
					event.event_str += name + "=" + value + "\n"
			log.debug("Event %s %s", event.event_type, event.job_id)

	def ReadCondorUserLog(self,fname):
		self.fname = fname
		filetime = time.localtime(os.path.getmtime(fname))
		F = open(fname,"r")
		while 1:
			line = F.readline()
			if not line: break

			if line[-1] == "\n":
				line = line[0:-1]
				if not line: continue

			if line[0] == "<" and len(self.events) == 0:
				self.ReadCondorUserLogXML(fname)
				return

			fields = string.split(line)
			event_type = long(fields[0])
			job_id = fields[1]
			if job_id and job_id[0] == "(" and job_id[-1] == ")":
				job_id = job_id[1:-1]
			if len(job_id) > 4 and job_id[-4:] == ".000":
				job_id = job_id[0:-4]
			event = CondorUserLogEvent()
			self.events.append(event)
			event.event_type = event_type
			event.job_id = job_id

			if event_type == EXECUTING_EVENT:
				event.RemoteHost = fields[8]

			date_str = fields[2]
			time_str = fields[3]
			t = time.strptime(str(filetime.tm_year) + "/" + date_str + " " + time_str, "%Y/%m/%d %H:%M:%S")
			event.timestamp = time.mktime(t)

			event_str = line
			while 1:
				line = F.readline()
				if not line:
					break
				if line[-1] == "\n":
					line = line[0:-1]
				if line == "...":
					break
				event_str = event_str + "\n" + line
			event.event_str = event_str

			if event.event_type == JOB_AD_INFORMATION_EVENT:
				for line in event_str.split("\n")[1:]:
					fields = line.split("=",1)
					if len(fields) == 2:
						key = fields[0].strip()
						value = fields[1].strip()
						if key == "MachineAttrGLIDEIN_Site0":
							event.MachineAttrGLIDEIN_Site0 = unquoteClassAdString(value)
						elif key == "MachineAttrName0":
							event.MachineAttrName0 = unquoteClassAdString(value)
						elif key == "Size":
							event.ImageSize = long(value)

			if event.event_type == TERMINATED_EVENT:
				line = event_str.split("\n")[1]
				m = re.match(".*Normal termination \\(return value ([0-9]+)\\).*",line)
				if m:
					event.ExitCode = long(m.groups(0)[0])
				else:
					m = re.match(".*Abnormal termination \\(signal ([0-9]+)\\).*",line)
					if m:
						event.ExitSignal = long(m.groups(0)[0])
					else:
						sys.stderr.write("Failed to get job exit status: " + line + "\n")

			if event.event_type == EVICTED_EVENT:
				for line in event_str.split("\n")[1:]:
					m = re.match(".*Normal termination \\(return value ([0-9]+)\\).*",line)
					if m:
						event.ExitCode = long(m.groups(0)[0])
					else:
						m = re.match(".*Abnormal termination \\(signal ([0-9]+)\\).*",line)
						if m:
							event.ExitSignal = long(m.groups(0)[0])


def sort_keys(dict):
	result = []
	for v,k in sorted([(v,k) for k,v in dict.items()]):
		result.append(k)
	return result

class CondorUserLogStats(object):
	good_hours = 0        # wall hours spent on successful run attempts
	bad_hours = 0         # wall hours spent on unsuccessful run attempts (not counting preempted run attempts)
	preempted_hours = 0   # wall hours spent on preempted run attempts

	good_jobs = 0         # number of jobs that succeeded
	bad_jobs = 0          # number of jobs that failed
	bad_runs = 0          # number of run attempts that failed (not counting preempted run attempts)

	site_bad_hours = None
	site_preempted_hours = None
	site_good_hours = None
	site_bad_runs = None
	site_bad_jobs = None
	site_good_jobs = None
	site_image_size = None
	site_image_size_count = None

	# the machine dictionaries are indexed by "site,machine"
	machine_bad_hours = None
	machine_preempted_hours = None
	machine_good_hours = None
	machine_bad_runs = None
	machine_bad_jobs = None
	machine_good_jobs = None
	machine_bad_job_ids = None
	machine_bad_run_ids = None

	def __init__(self):

		self.site_bad_hours = {}
		self.site_preempted_hours = {}
		self.site_good_hours = {}
		self.site_bad_runs = {}
		self.site_bad_jobs = {}
		self.site_good_jobs = {}
		self.site_image_size = {}
		self.site_image_size_count = {}

		self.machine_bad_hours = {}
		self.machine_preempted_hours = {}
		self.machine_good_hours = {}
		self.machine_bad_runs = {}
		self.machine_bad_jobs = {}
		self.machine_good_jobs = {}
		self.machine_bad_job_ids = {}
		self.machine_bad_run_ids = {}

	def __str__(self):
		s = ""

		s += "%15s %11s %11s %11s %11s %11s %11s %11s\n" % ("Site", "Success",      "", "Failed",      "",     "", "Preempted", "VSIZE")
		s += "%15s %11s %11s %11s %11s %11s %11s %11s\n" % (    "",    "Jobs", "Hours",  "Jobs",  "Runs", "Hours", "Hours",     " (MB)")
		s += "%15s %11s %11s %11s %11s %11s %11s %11s\n" % ("----",    "----", "-----",  "----",  "----", "-----", "-----",     "-----")

		avg_image_size = 0
		image_size_count = 0
		for site in self.site_image_size.keys():
			avg_image_size += self.site_image_size[site]
			image_size_count += self.site_image_size_count[site]
		if image_size_count:
			avg_image_size = avg_image_size/image_size_count

		s += "%15s %11s %11s %11s %11s %11s %11s %11s\n" % (
			"TOTAL",
			self.good_jobs,
			long(round(self.good_hours)),
			self.bad_jobs,
			self.bad_runs,
			long(round(self.bad_hours)),
			long(round(self.preempted_hours)),
			avg_image_size/1024
		)

		sorted_sites = sort_keys(self.site_good_jobs)
		sorted_sites.reverse()
		for site in sorted_sites:
			s += "%15s %11s %11s %11s %11s %11s %11s %11s\n" % (
				site,
				self.site_good_jobs.get(site,0),
				long(round(self.site_good_hours.get(site,0))),
				self.site_bad_jobs.get(site,0),
				self.site_bad_runs.get(site,0),
				long(round(self.site_bad_hours.get(site,0))),
				long(round(self.site_preempted_hours.get(site,0))),
				self.site_image_size.get(site,0)/self.site_image_size_count.get(site,1)/1024
			)

		sorted_machines = sort_keys(self.machine_bad_runs)
		sorted_machines.reverse()
		bad_machine_header_printed=0
		for site_machine in sorted_machines[0:5]:
			site,machine = site_machine.split(",",1)

			if self.machine_bad_runs.get(site_machine,0) == 0:
				continue
			if not bad_machine_header_printed:
				bad_machine_header_printed=1
				s += "\nMachines with the most failures:\n"
				s += "%15s %15s %11s %11s %11s %11s  %s\n" % ("Machine","Site","Failed",    "",     "","Preempted","Sample")
				s += "%15s %15s %11s %11s %11s %11s  %s\n" % (       "",    "",  "Jobs","Runs","Hours","Hours",    "Jobs IDs")
				s += "%15s %15s %11s %11s %11s %11s  %s\n" % ("-------","----","------","----","-----","-----",    "--------")

			bad_job_ids = self.machine_bad_job_ids.get(site_machine,[])[0:3]
			if not bad_job_ids:
				bad_job_ids = self.machine_bad_run_ids.get(site_machine,[])[0:3]

			s += "%15s %15s %11s %11s %11s %11s  %s\n" % (
				machine,
				site,
				self.machine_bad_jobs.get(site_machine,0),
				self.machine_bad_runs.get(site_machine,0),
				long(round(self.machine_bad_hours.get(site_machine,0))),
				long(round(self.machine_preempted_hours.get(site_machine,0))),
				string.join(bad_job_ids,",")
			)

		return s

	def add(self,ulog):
		site = ""
		machine = ""
		job_image_size = {}
		started_execution = {}
		for event in ulog.events:
			if event.MachineAttrGLIDEIN_Site0:
				site = event.MachineAttrGLIDEIN_Site0
			if event.RemoteHost:
				machine = event.RemoteHost
				pos = machine.find(":")
				if pos >= 0 and machine[0] == "<":
					machine = machine[1:pos]
			if event.MachineAttrName0:
				machine = event.MachineAttrName0
				pos = machine.rfind("@")
				if pos >= 0:
					machine = machine[pos+1:]
			if event.ImageSize:
				job_image_size[event.job_id] = event.ImageSize

			if event.event_type == EXECUTING_EVENT:
				started_execution[event.job_id] = event.timestamp

			if event.event_type == TERMINATED_EVENT or event.event_type == EVICTED_EVENT or event.event_type == RECONNECT_FAILED_EVENT:
				good_job = 0
				bad_job = 0
				bad_run = 0
				runtime = 0
				good_hours = 0
				bad_hours = 0
				preempted_hours = 0

				started = started_execution.get(event.job_id,0)
				if started:
					runtime = (event.timestamp - started)/3600.0
					del started_execution[event.job_id]
				else:
					# This is an expected case.  It can happen when a glidein gets killed
					# during stage-in.  The shadow eventually fails to reconnect and
					# logs RECONNECT_FAILED_EVENT without ever having logged
					# EXECUTING_EVENT.
					# We end up counting this run attempt as 0 runtime, even though
					# the job may have been tied up trying to reconnect for the full
					# job lease duration.  It is likely that not much time was consumed
					# on the execute machine.
					#sys.stderr.write("WARNING: termination event is not preceded by execution event in " + ulog.fname + "\n")
					pass

				if event.event_type == EVICTED_EVENT or event.event_type == RECONNECT_FAILED_EVENT:
					if event.ExitCode == None:
						preempted_hours = runtime
					else:
						bad_run = 1
						bad_hours = runtime
				elif event.ExitCode != 0:
					bad_job = 1
					bad_run = 1
					bad_hours = runtime
				else:
					good_job = 1
					good_hours = runtime

				self.good_jobs += good_job
				self.good_hours += good_hours
				self.bad_jobs += bad_job
				self.bad_runs += bad_run
				self.bad_hours += bad_hours
				self.preempted_hours += preempted_hours
				if not site:
					site = "other"
				self.site_good_jobs[site] = self.site_good_jobs.get(site,0) + good_job
				self.site_bad_runs[site] = self.site_bad_runs.get(site,0) + bad_run
				self.site_bad_jobs[site] = self.site_bad_jobs.get(site,0) + bad_job
				self.site_preempted_hours[site] = self.site_preempted_hours.get(site,0) + preempted_hours
				self.site_bad_hours[site] = self.site_bad_hours.get(site,0) + bad_hours
				self.site_good_hours[site] = self.site_good_hours.get(site,0) + good_hours

				image_size = job_image_size.get(event.job_id,None)
				if image_size:
					self.site_image_size[site] = self.site_image_size.get(site,0) + image_size
					self.site_image_size_count[site] = self.site_image_size_count.get(site,0) + 1

				if not machine:
					machine = "other"

				m = site + "," + machine
				self.machine_good_jobs[m] = self.machine_good_jobs.get(m,0) + good_job
				self.machine_bad_runs[m] = self.machine_bad_runs.get(m,0) + bad_run
				self.machine_bad_jobs[m] = self.machine_bad_jobs.get(m,0) + bad_job
				self.machine_preempted_hours[m] = self.machine_preempted_hours.get(m,0) + preempted_hours
				self.machine_bad_hours[m] = self.machine_bad_hours.get(m,0) + bad_hours
				self.machine_good_hours[m] = self.machine_good_hours.get(m,0) + good_hours
				if bad_job:
					bad_job_ids = self.machine_bad_job_ids.get(m,None)
					if bad_job_ids == None:
						bad_job_ids = []
						self.machine_bad_job_ids[m] = bad_job_ids
					bad_job_ids.append(event.job_id)
				if bad_run and not bad_job:
					bad_run_ids = self.machine_bad_run_ids.get(m,None)
					if bad_run_ids == None:
						bad_run_ids = []
						self.machine_bad_run_ids[m] = bad_run_ids
					bad_run_ids.append(event.job_id)

def ReadCondorUserLog(fname):
	ulog = CondorUserLog()
	try:
		ulog.ReadCondorUserLog(fname)
	except:
		sys.stderr.write("Error reading Condor user log " + fname + "\n")
		raise
	return ulog

def main():
	function = "--has-pending-jobs"
	args = sys.argv[1:]
	if len(args) >= 2 and args[0].find("--")==0:
		function = args[0]
		args = args[1:]

	ulog = ReadCondorUserLog(args[0])
	sys.stdout.write(args[0] + "\n")

	if function == "--has-pending-jobs":
		if ulog.HasPendingJobs():
			log.warn("Pending jobs.")
			return 1
		else:
			log.debug("No pending jobs.")
	elif function == "--job-failed":
		if ulog.JobFailed():
			log.warn("Failed job.")
			return 0
		else:
			log.debug("Job not failed.")
			return 1
	else:
		log.warn("Unknown option: " + function)
		return 1

if __name__ == "__main__":
	try:
		sys.exit(main())
	except KeyboardInterrupt:
		sys.exit()
