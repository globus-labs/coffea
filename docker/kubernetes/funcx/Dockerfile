FROM centos:7

RUN yum update -y \
    && yum -y install https://centos7.iuscommunity.org/ius-release.rpm \
	    xrootd xrootd-server xrootd-client xrootd-client-devel xrootd-python \
    && yum clean all

FROM python:3.6

RUN pip install six
RUN pip install git+https://github.com/globus-labs/coffea.git@master
