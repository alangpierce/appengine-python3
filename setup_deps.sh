#!/bin/sh

# Install Python 3.4 on a Debian Wheezy image. Unfortunately, it can't be
# installed through apt-get, so we need to install it from source.

cd /home/vmagent
apt-get update
apt-get -y install sudo wget gcc make libssl-dev openssl
wget https://www.python.org/ftp/python/3.4.2/Python-3.4.2.tgz
tar -zxf Python-3.4.2.tgz
cd Python-3.4.2
./configure --prefix=/usr/local
make
make install
cd /home/vmagent
rm Python-3.4.2.tgz
rm -rf Python-3.4.2

# TODO(alan): Only remove these if they weren't already there.
apt-get -y remove wget gcc make libssl-dev openssl
apt-get -y autoremove

