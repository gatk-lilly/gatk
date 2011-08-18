#!/bin/bash

#if [ $# < 4 ]; then
#	echo "Must supply s3 paths to end1 fastq and end2 fastq, a final name for the aligned BAM file, and a read group string."
#	exit
#fi

export JAVA_HOME=$HOME/opt/jdk1.6.0_27/
export PATH=$HOME/opt/apache-ant-1.8.2/bootstrap/bin:$HOME/opt/apache-ant-1.8.2/dist/bin:$HOME/opt/jdk1.6.0_27/bin:$HOME/opt/jdk1.6.0_27/db/bin:$HOME/opt/jdk1.6.0_27/jre/bin:$HOME/opt/git-1.7.6/bin:$HOME/opt/git-1.7.6/perl/blib/bin:$HOME/opt/bwa-0.5.9:$HOME/opt/samtools-0.1.17:$PATH

export WORK=/mnt/scratch
export RESOURCES=$WORK/resources
export TMP=$WORK/tmp
export DATA=$WORK/data

export GATK="java -Djava.io.tmpdir=$TMP -jar $HOME/opt/GATK-Lilly/dist/GenomeAnalysisTK.jar";
export QUEUE="java -Djava.io.tmpdir=$TMP -jar $HOME/opt/GATK-Lilly/dist/Queue.jar";

cd $WORK

if [ ! -e $TMP ]; then
	mkdir $TMP
fi

if [ ! -e $WORK/resources/resources.tar.gz ]; then
	s3cmd sync s3://$S3_BUCKET/resources $WORK/
fi

if [ ! -e $RESOURCES/ucsc.hg19.fasta ]; then
	gunzip -c $RESOURCES/resources.tar.gz | tar -C $RESOURCES -xf -
fi

if [ ! -e $DATA ]; then
	mkdir $DATA
fi

if [ ! -e $DATA/end1.fq.gz ]; then
	s3cmd sync $1 $DATA/end1.fq.gz
fi

if [ ! -e $DATA/end2.fq.gz ]; then
	s3cmd sync $2 $DATA/end2.fq.gz
fi

if [ ! -e $3 ]; then
	$QUEUE -S $HOME/opt/GATK-Lilly/public/scala/qscript/org/broadinstitute/sting/queue/qscripts/LaneProcessingPipeline.scala -bwa $HOME/opt/bwa-0.5.9/bwa -R $RESOURCES/ucsc.hg19.fasta -f1 $DATA/end1.fq.gz -f2 $DATA/end2.fq.gz -name $3 -rg $4 -threads $5 $6
fi

LANE_DIR=`echo $3 | sed 's/.bam//'`

BAM=$3
S3_UPLOAD_BAM="$S3_BUCKET/lanes/ACRG/$LANE_DIR/";

BAI=`echo $3 | sed 's/.bam/.bai/'`
S3_UPLOAD_BAI=`echo $S3_UPLOAD_BAM | sed 's/.bam/.bai/g'`;

if [ ! -e bam.finished ]; then
	$HOME/bin/synchronize.sh --nodelete UP $S3_UPLOAD_BAM $BAM && touch bam.finished
fi

if [ ! -e bai.finished ]; then
	$HOME/bin/synchronize.sh --nodelete UP $S3_UPLOAD_BAI $BAI && touch bai.finished
fi

echo "Done."
