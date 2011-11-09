#!/bin/bash

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

export HOME=/shared/home/ngs-user
export JAVA_HOME=$HOME/opt/jdk1.6.0_27/
export PATH=$HOME/bin:$HOME/opt/Python-2.7.2/bin:$HOME/opt/aria2-1.13.0/bin:$HOME/opt/apache-ant-1.8.2/bootstrap/bin:$HOME/opt/apache-ant-1.8.2/dist/bin:$HOME/opt/jdk1.6.0_27/bin:$HOME/opt/jdk1.6.0_27/db/bin:$HOME/opt/jdk1.6.0_27/jre/bin:$HOME/opt/git-1.7.7.1/bin:$HOME/opt/git-1.7.7.1/perl/blib/bin:$HOME/opt/bwa-0.5.9:$HOME/opt/samtools-0.1.17:/usr/kerberos/bin:/usr/local/bin:/bin:/usr/bin:/opt/condor/current/bin:/usr/java/default/bin:$PATH

export WORK=/mnt/scratch/$SM/$ID
export RESOURCES=$WORK/resources
export TMP=$WORK/tmp
export DATA=$WORK/data

export GATK="java -Djava.io.tmpdir=$TMP -jar $HOME/opt/GATK-Lilly/dist/GenomeAnalysisTK.jar";
export QUEUE="java -Djava.io.tmpdir=$TMP -jar $HOME/opt/GATK-Lilly/dist/Queue.jar";
export CPUS=`cat /proc/cpuinfo | grep -c processor`

export BAM_BASE="$FLOWCELL.$LANE"
export BAM=$BAM_BASE.bam
export BAI=$BAM_BASE.bai

s3_path=`echo $S3_UPLOAD_PATH | sed 's/[A-Za-z_\-]*\///'`
s3_bucket=`echo $S3_UPLOAD_PATH | sed "s/\/.*//"`

export HOST=`hostname`
echo "Running on $HOST"

echo "Creating $WORK directory..."
rm -rf $WORK
mkdir -p $WORK
mkdir $DATA
mkdir $TMP

echo "Changing to $WORK directory..."
cd $WORK

echo "Extracting NGS resources"
 
python $HOME/bin/s3_down_file.py -b $s3_bucket -s resources/resources.tar.gz -d $RESOURCES  
python $HOME/bin/s3_down_file.py -b $s3_bucket -s resources/variant_calling_resources.tar.gz  -d $RESOURCES 

gunzip -c $RESOURCES/resources.tar.gz | tar -C $RESOURCES -xf -

echo "Downloading fastqs..."

s3_fq1_path=`echo $FQ1 | sed 's/s3:\/\/[A-Za-z_\-]*\///'`
s3_fq2_path=`echo $FQ2 | sed 's/s3:\/\/[A-Za-z_\-]*\///'`

python $HOME/bin/s3_down_file.py -b $s3_bucket -s $s3_fq1_path -d $DATA -f end1.fq.gz  
python $HOME/bin/s3_down_file.py -b $s3_bucket -s $s3_fq2_path -d $DATA -f end2.fq.gz  

echo "Running lane-level pipeline..."
$QUEUE -S $HOME/opt/GATK-Lilly/public/scala/qscript/org/broadinstitute/sting/queue/qscripts/LaneProcessingPipeline.scala -bwa $HOME/opt/bwa-0.5.9/bwa -R $RESOURCES/ucsc.hg19.fasta -f1 $DATA/end1.fq.gz -f2 $DATA/end2.fq.gz -name $BAM -rg "@RG\tID:$ID\tSM:$SM\tLB:$LB\tPU:$PU\tPL:$PL\tCN:$CN\tDT:$DT" -threads $CPUS -run


#fix read names for each lane level data, the bai name may not be right, added the following 6 lines
#$GATK -T PrintReads -R $RESOURCES/ucsc.hg19.fasta -I $BAM -addrg -o $Fixed_BAM
#$HOME/opt/samtools-0.1.17/samtools index $Fixed_BAM

#mv $BAM $BAM.tmp
#mv $BAI $BAI.tmp

#mv $Fixed_BAM $BAM
#mv $Fixed_BAM.bai $BAI

echo "Uploading results..."

python $HOME/bin/s3_upload_file.py $s3_bucket $BAM $s3_path 
python $HOME/bin/s3_upload_file.py $s3_bucket $BAI $s3_path 

echo "Done."
