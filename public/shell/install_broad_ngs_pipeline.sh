#!/bin/bash

export JAVA_HOME=$HOME/opt/jdk1.6.0_27/
export PATH=$HOME/opt/apache-ant-1.8.2/bootstrap/bin:$HOME/opt/apache-ant-1.8.2/dist/bin:$HOME/opt/jdk1.6.0_27/bin:$HOME/opt/jdk1.6.0_27/db/bin:$HOME/opt/jdk1.6.0_27/jre/bin:$HOME/opt/git-1.7.6/bin:$HOME/opt/git-1.7.6/perl/blib/bin:$HOME/opt/bwa-0.5.9:$HOME/opt/samtools-0.1.17:$HOME/opt/jets3t-0.8.1/bin:$PATH
export JETS3T_HOME=$HOME/opt/jets3t-0.8.1
alias ls='ls --color'

cd $HOME

if [ ! -e opt/ ]; then
	mkdir opt
fi

if [ ! -e opt/jdk1.6.0_27/bin/javac ]; then
	wget http://download.oracle.com/otn-pub/java/jdk/6u27-b07/jdk-6u27-linux-x64.bin -O opt/jdk-6u27-linux-x64.bin
	chmod 777 opt/jdk-6u27-linux-x64.bin

	cd opt
	echo 'y' > answers.txt
	./jdk-6u27-linux-x64.bin < answers.txt
	cd $HOME
fi

if [ ! -e opt/apache-ant-1.8.2-src.tar.gz ]; then
	wget http://www.reverse.net/pub/apache//ant/source/apache-ant-1.8.2-src.tar.gz -O opt/apache-ant-1.8.2-src.tar.gz
	gunzip -c opt/apache-ant-1.8.2-src.tar.gz | tar -C opt/ -xf -
	wget --no-check-certificate https://github.com/downloads/KentBeck/junit/junit-4.9b4.jar -O opt/apache-ant-1.8.2/lib/optional/junit-4.9b4.jar

	cd opt/apache-ant-1.8.2/
	./build.sh
	cd $HOME
fi

if [ ! -e opt/git-1.7.6.tar.bz2 ]; then
	wget http://kernel.org/pub/software/scm/git/git-1.7.6.tar.bz2 -O opt/git-1.7.6.tar.bz2
	bunzip2 -c opt/git-1.7.6.tar.bz2 | tar -C opt/ -xf -

	cd opt/git-1.7.6
	./configure --prefix=$HOME/ --without-curl
	make
	make install
	cd $HOME
fi

if [ ! -e opt/GATK-Lilly ]; then
	git clone git://github.com/gatk-lilly/gatk.git opt/GATK-Lilly

	cd opt/GATK-Lilly
	ant dist queue
	cd $HOME
fi

if [ ! -e opt/samtools-0.1.17.tar.bz2 ]; then
	wget http://sourceforge.net/projects/samtools/files/samtools/0.1.17/samtools-0.1.17.tar.bz2/download -O opt/samtools-0.1.17.tar.bz2
	bunzip2 -c opt/samtools-0.1.17.tar.bz2 | tar -C opt/ -xf -

	cd opt/samtools-0.1.17
	make
	cd $HOME
fi

if [ ! -e opt/bwa-0.5.9.tar.bz2 ]; then
	wget http://sourceforge.net/projects/bio-bwa/files/bwa-0.5.9.tar.bz2/download -O opt/bwa-0.5.9.tar.bz2
	bunzip2 -c opt/bwa-0.5.9.tar.bz2 | tar -C opt/ -xf -

	cd opt/bwa-0.5.9
	make
	cd $HOME
fi

if [ ! -e opt/picard-tools-1.51.zip ]; then
	wget http://sourceforge.net/projects/picard/files/picard-tools/1.51/picard-tools-1.51.zip/download -O opt/picard-tools-1.51.zip
	unzip opt/picard-tools-1.51.zip -d opt/
fi

if [ ! -e opt/jets3t-0.8.1.zip ]; then
	wget http://bitbucket.org/jmurty/jets3t/downloads/jets3t-0.8.1.zip -O opt/jets3t-0.8.1.zip
	unzip opt/jets3t-0.8.1.zip -d opt/

	cd opt/jets3t-0.8.1
	chmod 777 bin/*.sh
	cd $HOME
fi

echo "Done."
