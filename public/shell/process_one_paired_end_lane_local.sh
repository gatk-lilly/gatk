#!/bin/bash

#$ -N LaneProcessingPipeline
#$ -S /bin/bash
#$ -cwd
#$ -j y
#$ -m be

# From all here all comments are by Johny 

set -x

export ID=$1
export SM=$2
export LB=$3
export PU=$4
export PL=$5
export CN=$6
export DT=$7
export FLOWCELL=$8
export LANE=$9
export FQ1=${10}
export FQ2=${11}
export S3_BUCKET=`echo ${12} | sed 's/s3:\/\///' | sed 's/\/$//'`
export S3_UPLOAD_PATH=`echo ${13}/$SM/$FLOWCELL.$LANE | sed 's/s3:\/\///'`


#Johny:  Set HOME variable to the directory where you installed the tools. Set WORK variable to the /lrlhps/scratch/userid/$SM/$ID
#These are the only two change in this file.

#echo $HOME 
#/home/u9x8503
loc=c085541

#export HOME=/shared/home/lilly-collab
export HOME=/lrlhps/users/$loc/GATK
#/lrlhps/users/u9x8503/GATK/opt/GATK-Lilly/public/perl
export CONDOR_CONFIG=/opt/condor/current/etc/condor_config
export JAVA_HOME=$HOME/opt/jdk1.6.0_27/
export PATH=$HOME/opt/apache-ant-1.8.2/bootstrap/bin:$HOME/opt/apache-ant-1.8.2/dist/bin:$HOME/opt/jdk1.6.0_27/bin:$HOME/opt/jdk1.6.0_27/db/bin:$HOME/opt/jdk1.6.0_27/jre/bin:$HOME/opt/git-1.7.6/bin:$HOME/opt/git-1.7.6/perl/blib/bin:$HOME/opt/bwa-0.5.9:$HOME/opt/samtools-0.1.17:$HOME/opt/jets3t-0.8.1/bin:/usr/kerberos/bin:/usr/local/bin:/bin:/usr/bin:/opt/condor/current/bin:/usr/java/default/bin:/shared/home/lilly-collab/bin:$PATH

#export WORK=/mnt/scratch/$SM/$ID
#export WORK=/lrlhps/scratch/u9x8503/ngs/GATK/work/$SM/$ID
export WORK=/lrlhps/scratch/$loc/ACRG/work/$SM/$ID

export RESOURCES=$WORK/resources 
export TMP=$WORK/tmp
export DATA=$WORK/data

export GATK="java -Djava.io.tmpdir=$TMP -jar $HOME/opt/GATK-Lilly/dist/GenomeAnalysisTK.jar";
export QUEUE="java -Djava.io.tmpdir=$TMP -jar $HOME/opt/GATK-Lilly/dist/Queue.jar";
export CPUS=`cat /proc/cpuinfo | grep -c processor`

export BAM_BASE="$FLOWCELL.$LANE"
export BAM=$BAM_BASE.bam
export BAI=$BAM_BASE.bai

export HOST=`hostname`
echo "Running on $HOST"

#Path change ends 

echo "Creating $WORK directory..."
rm -rf $WORK
mkdir -p $WORK
mkdir $DATA
mkdir $TMP

#Johny added 
#Create output upload path S3_UPLOAD_PATH
if [ ! -e $S3_UPLOAD_PATH ]; then
	echo "Creating $S3_UPLOAD_PATH directory..."
	mkdir -p $S3_UPLOAD_PATH
fi

echo "Changing to $WORK directory..."
cd $WORK

echo "Extracting NGS resources"
#cloud s3cmd sync s3://$S3_BUCKET/resources $WORK/
#local 
#cp $S3_BUCKET/resources $WORK/
#gunzip -c $RESOURCES/resources.tar.gz | tar -C $RESOURCES -xf -
# new logic is to create a soft link
ln -s /lrlhps/data/genomics/ref/gatk resources

echo "Downloading fastqs..."
# s3cmd sync $FQ1 $DATA/end1.fq.gz
# s3cmd sync $FQ2 $DATA/end2.fq.gz
ln -s $FQ1 $DATA/end1.fq.gz
ln -s $FQ2 $DATA/end2.fq.gz

echo "Running lane-level pipeline..."
$QUEUE -S $HOME/opt/GATK-Lilly/public/scala/qscript/org/broadinstitute/sting/queue/qscripts/LaneProcessingPipeline.scala -bwa $HOME/opt/bwa-0.5.9/bwa -R $RESOURCES/ucsc.hg19.fasta -f1 $DATA/end1.fq.gz -f2 $DATA/end2.fq.gz -name $BAM -rg "@RG\tID:$ID\tSM:$SM\tLB:$LB\tPU:$PU\tPL:$PL\tCN:$CN\tDT:$DT" -threads $CPUS -run

echo "Uploading results..."
mv $BAM $S3_UPLOAD_PATH 
mv $BAI $S3_UPLOAD_PATH 

echo "Done."
