%define name GratiaReporting
%define version 0.4.0
%define release 3

Summary: Gratia reporting package.
Name: %{name}
Version: %{version}
Release: %{release}%{?dist}
# Create using:
# python setup.py sdist
Source0: %{name}-%{version}.tar.gz
License: GPLv2+
Group: Development/Libraries
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-buildroot
Prefix: %{_prefix}
BuildArch: noarch
Vendor: Brian Bockelman <bbockelm@cse.unl.edu>
Requires: MySQL-python

%description
A collection of reports to be run against the Gratia accounting database

%prep
%setup

%build
python setup.py build

%install
python setup.py install --root=$RPM_BUILD_ROOT --record=INSTALLED_FILES

%clean
rm -rf $RPM_BUILD_ROOT

%files -f INSTALLED_FILES
%defattr(-,root,root)

%changelog
* Sat Sep 01 2012 Brian Bockelman <bbockelm@cse.unl.edu> - 0.4.0-3
- Rebuild with proper spec file instead of auto-generated one.


