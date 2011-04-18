#!/usr/bin/env python2.6
import datetime, time, os, yaml, getpass
from socket import gethostname
from syslog import *

ROOT = os.path.abspath(os.path.dirname(__file__))
ROOT_PATH = lambda *args: os.path.join(ROOT, *args)

class Work:
  COPY = 0
  LS = 1

class Commands:
  def __init__(self):
    self.commands = [] 
  
  def add(self, command, *value):
    self.commands.append(str(command))
    for v in value:
      self.commands.append(str(v))

  def __str__(self):
    return " ".join(self.commands)

def write_log(message, priority = LOG_INFO):
  openlog("PFTOOL-LOG", 0, LOG_USER)
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

def parse_config(options_path=ROOT_PATH("config", "pftool.yaml")):
  f = open(options_path)
  results = yaml.load(f)
  f.close()
  return results

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
