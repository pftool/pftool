#!/usr/bin/env python2.6
import datetime, time, os, getpass
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
