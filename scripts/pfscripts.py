#!/usr/bin/env python2.6
import datetime, time, os, getpass, re
from socket import gethostname
from syslog import *

import ConfigParser
import socket

ROOT = os.path.abspath(os.path.dirname(__file__))
ROOT_PATH = lambda *args: os.path.join(ROOT, *args)

"""
checks if ptool is in the environment and uses that, otherwise uses ../bin/pftool
"""
pftool=os.getenv('PFTOOL', ROOT_PATH("..","bin","pftool"))
procs_per_node_default = 1
nodes_per_job_default = 16

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

def parse_config(options_path=os.getenv('PFTOOL_CONFIG', ROOT_PATH("..", "etc", "pftool.cfg"))):
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

def is_ssh_running(host):
    reachable = False
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(.1)

    try:
        s.connect((host, 22))
        reachable = True
    except socket.error as e:
        remove = 1
    s.close()
    return reachable

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
def sbatch_submit(config, options, output_path, pfcmd, commands):
	try:
		procs_per_node = config.get('options', 'procs_per_node')
	except:
		print('procs_per_node is not specified in pftool config file, using default value %d' % procs_per_node_default)
		procs_per_node = procs_per_node_default

	try:
		nodes_per_job = config.get('options', 'nodes_per_job')
	except:
		print('nodes_per_job is not specified in pftool config file, using default value %d' % nodes_per_job_default)
		nodes_per_job = nodes_per_job_default

	pfcmd.add(pftool)
	pfcmd.add(*command.commands)
	pfcmd_str = ''
	for i in range(0, len(pfcmd.commands))
		pfcmd_str = pfcmd_str + pfcmd.commands[i] + ' '

	if options.debug:
		print(pfcmd_str)

	lines = []
	lines.append('#!/usr/bin/env bash\n')
	lines.append('#SBATCH --output={}/%j.out\n'.format(output_path))
	lines.append('#SBATCH --nodes={}\n'.format(nodes_per_job))
	lines.append('#SBATCH --ntasks-per-node={}\n'.format(procs_per_node))
	lines.append('#SBATCH --time=0') #for interactive job, no time limit
	lines.append('mpirun {}'.format(pfcmd_str))

	job_script_path = output_path + '/job_script.sh'
	fd = open(job_script_path, 'w+')
	for i in range(0, len(lines)):
		fd.write(lines[i])
	fd.close()

	try:
		slurm_path = config.get('environment', 'slurm_exe_dir')
	except:
		parser.error('slurm executable directory is not specified in pftool config file')

	slurm_path = slurm_path + '/sbatch'
	args = []
	args.append(slurm_path)
	args.append(job_script_path)
	sbatch_return = subprocess.check_output(args)
	print(sbatch_return)
	words = sbatch_return.split(' ')

	if ('failed' in words) or ('fail' in words):
		print('failed to submit pftool job to slurm')
		sys.exit(1)

	job_id = None
	for i in range(0, len(words)):
		if words[i] == 'job':
			job_id = words[i+1]

	return job_id

def get_job_status(squeue_path, job_id):
	args = []
	args.append(squeue_path)
	args.append('-j')
	args.append(job_id)
	squeue_out = subprocess.check_output(args)
	splits = squeue_out.split('\n')
	if len(splits) > 1:
		tokens = splits[1].split(' ')
		return tokens[4]
	else:
		return None

def fg_output(pf_type, config, slurm_output_dir, job_id):
	slurm_path = config.get('environment', 'slurm_exe_dir')
	squeue_path = slurm_path + '/squeue'
	output_file = slurm_output_dir + '/{}.out'.format(job_id)
	running = 0
	args = []
	args.append(squeue_path)
	args.append('-j')
	args.append(job_id)
	while running == 0:
		time.sleep(30)
		status = get_job_status(squeue_path, job_id)
		if status == None:
			continue
		elif status == 'R' or status == 'RUNNING':
			running = 1
		elif status == 'PD' or status == 'PENDING':
			print('Your %s job is still pending' % pf_type)
		elif status == 'F' or status == 'FAILED':
			print('Your %s job failed' % pf_type)
			running = -1
		elif status == 'CA' or status == 'CANCELLED':
			print('Your %s job is canceled' % pf_type)
			running = -1
	if running != 1:
		sys.exit(1)

	#start reading output file
	done = 0
	interval_cnt = 0
	read_size = 0
	prev_size = 0
	while True:
		#update output 5 times
		time.sleep(5)
		try:
			st = os.stat(output_file)
		except OSError:
			print('Error reading output file\nIf this persists, use CTRL+c to exit')
			continue

		current_size = st.st_size
		if prev_size == current_size and done == 0:
			#nothing new to read, skip
			continue
		elif prev_size == current_size and done != 0:
			break
		#read the newly written part of the file and print all the lines.
		#If buf has a partial line at the end, save for next cycle
		read_size = current_size - prev_size
		fd = open(output_file, 'r')
		fd.seek(prev_size, 0)
		buf = fd.read(read_size)
		fd.close()
		print_line = None
		if done == 0:
			new_line_index = buf.rfind('\n')
			print_line = buf[0:new_line_index+1]
			prev_size = prev_size + new_line_index + 1
		else:
			print_line = buf
			
		print(print_line)
		interval_cnt = interval_cnt + 1
		if done != 0:
			break
		if (interval_cnt % 10) == 0:
			#need to check job status
			status = get_job_status(squeue_path, job_id)
			if status == 'CD' or status == 'COMPLETED':
				done = 1
			elif status == 'CA' or status == 'CANCELLED' or \
				status == 'F' or status == 'FAILED' or \
				status == 'PR' or status == 'PREEMPTED' or \
				status == 'DL' or status == 'DEADLINE':
				done = -1
	#job finished, cleanup
	if done == -1:
		print('Your %s job failed' % pf_type)

	os.unlink(output_file)
	os.rmdir(slurm_output_dir)
	sys.exit(0)

def run_with_slurm(pf_type, config, options, pfcmd, commands):
	#first get output file directory path
	user_home_dir = os.environ['HOME']
	#check if output dir exists
	slurm_output_dir = user_home_dir + '/.pf_out'
	if os.path.isdir(slurm_output_dir) == False:
		#we must create output directory
		os.mkdir(slurm_output_dir, 0755)
	#now we can specify slurm output file
	slurm_output_file = slurm_output_dir + 'pf_output'
	job_id = sbatch_submit(config, options, slurm_output_path, pfcmd, commands)
	if options.fg == 0:
		#running in the back ground
		print('Your %s job has been submitted to slurm with job id %s' % (pf_type, job_id))
		print('You can check your job status using squeue')
		print('You can also check job progress by reading your job\'output file at %s' % (slurm_output_dir+('/{}.out').format(job_id)))
		print('You must delete the slurm output file at %s' % (slurm_output_dir+('/{}.out').format(job_id)))
	else:
		fg_output(pf_type, slurm_output_dir, job_id)
