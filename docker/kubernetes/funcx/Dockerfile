FROM opensciencegrid/osgvo-el7

RUN yum update -y \
    && yum install -y https://centos7.iuscommunity.org/ius-release.rpm \
    && yum install -y python36u python36u-libs python36u-devel python36u-pip

RUN ln -s /usr/bin/pip3.6 /bin/pip
RUN rm /usr/bin/python
RUN ln -s /usr/bin/python3.6 /usr/bin/python

RUN pip install six
RUN pip install parsl==0.9.0
RUN pip install git+https://github.com/globus-labs/coffea.git@master
RUN pip install git+https://github.com/funcx-faas/funcX.git@zz_kube_container_adv



CMD [ "/bin/bash" ]#, "-c", "--" ]
