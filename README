###############################################################################
          PFTool: Parallel File Tool
###############################################################################

PFTool (Parallel File Tool) can stat, copy, and compare files in parallel. 
It's optimized for an HPC workload and uses MPI for message passing.

Additional info:
Available under LANL LACC-2012-072.
see COPYRIGHT file

***************************************************************************
Dependencies
***************************************************************************

autoconf
automake
m4
libtool
mpi

***************************************************************************
Installing PFTool
***************************************************************************
From the top-level directory

./autogen

#To configure the base version:
./configure

#To configure the threaded version:
#./configure --enable-threads

#Other options:
#./configure --help

./make clean
./make all
./make install

Note that pftool.cfg is created and installed into {install_prefix}/etc
during the "make install". Setting in this file should be reviewed and 
modified based on the configuration of the cluster the software is
running on. 

***************************************************************************
PFTool RPM
***************************************************************************
To build an RPM, from the top-level directory:

cd package
make rpm

Rpmbuild is used by this make file to generate the RPM. It is assumed that
the directory/tree $HOME/rpmbuild exists and has the subdirectories SOURCES,
BUILD, and RPMS. The resulting RPM is written to $HOME/rpmbuild/RPMS.

***************************************************************************
Configuration
***************************************************************************
{install_prefix}/etc/pftool.cfg is read by the pftool scripts pfls, pfcm, pfcp

example config files located in ./etc/. 

***************************************************************************
Versioning
***************************************************************************
In order to change the version number for this project, modify the following
line in ./configure.ac:

AC_INIT([pftool], [2.0.5], [dsherril@lanl.gov])

Then rerun

./configure

Note that the maintainer information can be changed as well.

***************************************************************************
Using PFtool
***************************************************************************
PFTool can be invoked directly, but the preferred method is through helper
scripts located in {install_prefix}/scripts/

-----------

Usage: pfls [options] sourcePath

pfls --  list file(s) based on sourcePath in parallel

Options:
  -h, --help     show this help message and exit
  -R             list directories recursively
  -v             verbose result output
  -i INPUT_LIST  input file list

-----------

Usage: pfcm [options] sourcePath destinationPath

pfcm -- compare file(s) from sourcePath to destinationPath in parallel

Options:
  -h, --help  show this help message and exit
  -R          copy directories recursively
  -M          changes to Block-by-Block vs Metadata only
  -v          verbose result output

-----------

Usage: pfcp [options] sourcePath destinationPath

pfcp -- copy file(s) from sourcePath to destinationPath in parallel

Options:
  -h, --help  show this help message and exit
  -R          copy directories recursively
  -v          verbose result output
  -n          only copy files that have a different date or file size than the
              same files at the destination or not in the destination


