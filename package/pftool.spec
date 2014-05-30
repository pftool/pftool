%define name pftool
%define version 2.0.5
%define release 1

%define debug_package %nil
%define _prefix /usr/local

Name:		%{name}		
Version:	%{version}	
Release:	1%{?dist}
Summary: 	PFTool (Parallel File Tool) can stat, copy, and compare files in parallel.
Group: 		Applications/File
License:	LANL	
Vendor:		LANL HPC-3
Packager:	HPC-3 Infrastructure (David Sherrill <dsherril@lanl.gov>)
Excludeos:	windows
Prefix:		%{_prefix}
Source0:	%{name}-%{version}.tar.gz
BuildRoot:	%(mktemp -ud %{_tmppath}/%{name}-%{version}-%{release}-XXXXXX)

# Should have MPI RPMs required. However TOSS/openmpi naming convention breaks a simple comparison
# Requirement handled by loading module (for build) and ensuring that mpirun is in user's path
#BuildRequires: openmpi >= 1.6
#Requires:	openmpi >= 1.6	

%description
PFTool (Parallel File Tool) can stat, copy, and compare files in parallel. 
It's optimized for an HPC workload and uses MPI for message passing.
A threaded version is available and can be configured at build time.

%prep
%setup -q


%build
module load openmpi
%configure
make %{?_smp_mflags}


%install
rm -rf %{buildroot}
mkdir -p %{buildroot}/%{prefix}/etc
make install DESTDIR=%{buildroot}
# Modifies pftool.cfg to use all available nodes
mv  %{buildroot}/%{prefix}/etc/pftool.cfg  %{buildroot}/%{prefix}/etc/pftool.cfg.rpm
sed -e "s/^\#all:/all:/" -e "s/^localhost:/#localhost:/" %{buildroot}/%{prefix}/etc/pftool.cfg.rpm > %{buildroot}/%{prefix}/etc/pftool.cfg
rm -f %{buildroot}/%{prefix}/etc/pftool.cfg.rpm



%clean
rm -rf %{buildroot}


%files
%defattr(-,root,root,-)
%{prefix}/bin/
%{prefix}/etc/
%doc



%changelog
* Wed May 29 2014 David Sherrill <dsherril@lanl.gov>
  - first cut.
