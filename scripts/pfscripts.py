#!/usr/bin/env python2.6
import datetime, time, os, getpass, re
from socket import gethostname
from syslog import *

import ConfigParser


ROOT = os.path.abspath(os.path.dirname(__file__))
ROOT_PATH = lambda *args: os.path.join(ROOT, *args)
pftool = ROOT_PATH("..", "bin", "pftool")

class Work:
  COPY = 0
  LS = 1
  COMPARE = 2

class Commands:
  def __init__(self):
    self.commands = [] 
  
  def add(self, command, *value):
    self.commands.append(str(command))
    for v in value:
      self.commands.append(str(v))

  def __str__(self):
    return " ".join(self.commands)

def write_log(message, priority = LOG_ERR | LOG_USER):
  openlog("PFTOOL-LOG", LOG_PID | LOG_CONS, LOG_USER)
  syslog(priority, message)
  closelog()

def get_jid():
  user = getpass.getuser()
  c = time.gmtime()
  
  time_id = "%d%d%d%d%d%d"%(c.tm_sec, c.tm_min, c.tm_hour, c.tm_mday, 
      c.tm_mon, c.tm_year)

  hostname = gethostname()
  jid = user+time_id+hostname
  return jid

def parse_config(options_path=ROOT_PATH("..", "etc", "pftool.cfg")):
  config = ConfigParser.ConfigParser()
  config.read(options_path)
  return config

def findexec(executable, path=None):
  """
  Try to find 'executable' in the directories listed in 'path' (a
  string listing directories separated by 'os.pathsep'; defaults to
  os.environ['PATH']).  Returns the complete filename or None if not
  found
  """
 
  if executable[0] == os.pathsep and os.access(executable, os.X_OK):
    return executable

  if path is None:
    path = os.environ['PATH']
  paths = path.split(os.pathsep)

  for dir in paths:
    fullexec = os.path.join(dir,executable)
    try:
      st = os.stat(fullexec)
    except os.error:
      continue
    if os.path.exists(fullexec) and os.access(fullexec, os.X_OK):	# is executable
      return fullexec
  return None

def get_nodeallocation():
  """
  This function reads the environment to see if any variables
  are set that give an indication of what nodes/processes are
  currently allocated for the code running. The 2-tuple (nodelist,numprocs)
  is returned. If no allocation is set, then ([],0) is
  returned

  This function currently supports the MOAB (PBS_NODEFILE) and
  SLURM job control environments. Currently the SLURM environment
  take precedence over MOAB

  This function parses SLURM_JOB_NODELIST values such as:
    yfta03
    r-fta[04,12,20]
    cslic[1,2-4,7]
    lynx[02-04,07]s
  """

  nodelist = []								# the list of nodes in the allocation
  numprocs = 0								# total number of processors/processes for the job

	# check Environment for Job control variables
  try:									# check for SLURM
    slurm_nodes = os.environ['SLURM_JOB_NODELIST']
    slurm_ppn = os.environ['SLURM_CPUS_ON_NODE']
		# parse the node list variable
    if re.match("[a-zA-Z-]+[0-9]*[a-zA-Z-]*$",slurm_nodes) is not None:	# examples of cases matched: fta04, r-fta05, r-b-node, fta, fta003sb
      nodelist.append(slurm_nodes)					# just one node in list
    else:								# examples of cases matched: fta[03-06], fta[05,07,09], fta[01-04,07,09,10-12]
      mobj = re.match("([a-zA-Z-]+)\[((([0-9]+(\-[0-9]+)*)\,*)+)\]([a-zA-Z-]*)$",slurm_nodes)

      if mobj is None:							# not a valid SLURM_JOB_NODELIST value -> get out of here!
        raise KeyError					

      npre = mobj.group(1)						# Group 1 is the node name prefix (i.e. fta)
      nnum = mobj.group(2).split(',')					# Group 2 is a list of the node numbers (i.e. 01-04,07)
      nsuf = mobj.group(6)						# Group 6 is the node name suffix after any numbers (i.e. s)
      for n in nnum:
          nums = n.split('-')
          if len(nums) < 2:						# not a range of numbers
            if len(nsuf):						# see if we have a node name suffix
              nodelist.append(npre + nums[0] + nsuf)
            else:
              nodelist.append(npre + nums[0])
          else:								# a range is specified
            low = int(nums[0])
            high = int(nums[1])+1
            maxdigits = len(nums[1])

            if high < low:						# paranoid check. If true -> something is terribly wrong!
              raise KeyError
            for i in range(low,high):					# iterate through range, adding nodes to list
              if len(nsuf):
                nodelist.append("%s%0*d%s" % (npre,maxdigits,i,nsuf))
              else:
                nodelist.append("%s%0*d" % (npre,maxdigits,i))

    numprocs = len(nodelist) * int(slurm_ppn)				# compute processors/processes for the job
  except KeyError:
    nodelist = []
    numprocs = 0

  if not len(nodelist):							# SLURM was a no-go
    try:								# check MOAB
      moab_nodes = os.environ['PBS_NODEFILE']
      if not os.path.exists(moab_nodes):
        raise KeyError
      n_fd = open(moab_nodes,'r')
      n_line = n_fd.readline()						# each line should be a node name
      while n_line != "":
        n_line = n_line.strip()						# get ride of newline ....
        if n_line not in nodelist:					# WARNING: this does a sequencial search over nodelist
          nodelist.append(n_line)
        numprocs = numprocs + 1 					# total number of processors/processes are the number of lines in the file
        n_line = n_fd.readline()
      n_fd.close()
    except KeyError:
      nodelist = []
      numprocs = 0

  return(nodelist,numprocs)

def add_darshan(pfconfig, mpicmd):
    # If darshan is specified in the environment and valid, add it to the mpi command line
    try:
      darshanlib = pfconfig.get("environment", "darshanlib");
      if os.access(darshanlib, os.R_OK):
        new_preload = darshanlib
        orig_preload = os.environ.get("LD_PRELOAD")
        if orig_preload:
           new_preload +=  ":" + orig_preload
        darshan_preload = "LD_PRELOAD=" + new_preload
        mpicmd.add("-x", darshan_preload)
    except:
      pass


def busy(): 
    print"""
*******************************************************************
*                                                                 *
* The Parallel Archive System is busy now. There is no available  *
* host machine to run your pfcm job now.  Please try it later.    *
*                                                                 *
* Contact:  ICN Consulting Office (5-4444 option 3)               *
*******************************************************************
"""
