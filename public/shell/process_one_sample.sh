#!/bin/bash

#$ -N LaneProcessingPipeline
#$ -S /bin/bash
#$ -cwd
#$ -j y
#$ -m be

#set -x

export SM=$1
export S3_BUCKET=`echo $2 | sed 's/s3:\/\///' | sed 's/\/$//'`
export S3_UPLOAD_ROOT=`echo $3 | sed 's/s3:\/\///' | sed 's/\/$//'`
export S3_UPLOAD_PATH="$S3_UPLOAD_ROOT/$SM"
export LANES=${@:4:$#}

export JAVA_HOME=$HOME/opt/jdk1.6.0_27/
export PATH=$HOME/opt/apache-ant-1.8.2/bootstrap/bin:$HOME/opt/apache-ant-1.8.2/dist/bin:$HOME/opt/jdk1.6.0_27/bin:$HOME/opt/jdk1.6.0_27/db/bin:$HOME/opt/jdk1.6.0_27/jre/bin:$HOME/opt/git-1.7.6/bin:$HOME/opt/git-1.7.6/perl/blib/bin:$HOME/opt/bwa-0.5.9:$HOME/opt/samtools-0.1.17:$HOME/opt/jets3t-0.8.1/bin:$PATH
export JETS3T_HOME=$HOME/opt/jets3t-0.8.1

export WORK=/mnt/scratch/$SM/aggregation
export RESOURCES=$WORK/resources
export TMP=$WORK/tmp
export DATA=$WORK/data
export LISTS=$WORK/lists

export GATK="java -Djava.io.tmpdir=$TMP -jar $HOME/opt/GATK-Lilly/dist/GenomeAnalysisTK.jar";
export QUEUE="java -Djava.io.tmpdir=$TMP -jar $HOME/opt/GATK-Lilly/dist/Queue.jar";
export CPUS=`cat /proc/cpuinfo | grep -c processor`

export BAM=$SM.bam
export BAI=$SM.bai
export BAM_LIST="$LISTS/$SM.bam.list"

#echo "Creating $WORK directory..."
#rm -rf $WORK
#mkdir -p $WORK
#mkdir $DATA
#mkdir $TMP
#mkdir $LISTS
#
#echo "Changing to $WORK directory..."
cd $WORK
#
#echo "Extracting NGS resources..."
#s3cmd sync s3://$S3_BUCKET/resources $WORK/
#gunzip -c $RESOURCES/resources.tar.gz | tar -C $RESOURCES -xf -
#
#echo "Downloading lane BAMs..."
#for LANE_BAM in $LANES
#do
#	LANE_BAI=`echo $LANE_BAM | sed 's/.bam/.bai/'`
#
#	BAM_BASENAME=`basename $LANE_BAM`
#	BAI_BASENAME=`basename $LANE_BAI`
#
#	s3cmd sync $LANE_BAM $DATA/$BAM_BASENAME
#	s3cmd sync $LANE_BAI $DATA/$BAI_BASENAME
#done
#find $DATA -name \*.bam > $BAM_LIST

echo "Running sample-level pipeline..."
$QUEUE -S $HOME/opt/GATK-Lilly/public/scala/qscript/org/broadinstitute/sting/queue/qscripts/DataProcessingPipeline.scala -i $BAM_LIST -r $HOME/opt/GATK-Lilly/public/R/ -R $RESOURCES/ucsc.hg19.fasta -D $RESOURCES/dbsnp_132.hg19.vcf -indels $RESOURCES/1000G_indels_for_realignment.hg19.vcf -p aggregated -run
mv aggregated.$SM.bam $BAM
mv aggregated.$SM.bai $BAI

echo "Uploading results..."
$JETS3T_HOME/bin/synchronize.sh --nodelete UP $S3_UPLOAD_PATH $BAM
$JETS3T_HOME/bin/synchronize.sh --nodelete UP $S3_UPLOAD_PATH $BAI

echo "Done."
