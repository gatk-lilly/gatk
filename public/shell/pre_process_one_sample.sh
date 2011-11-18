#!/bin/bash

export SM=$1
export CHR_LIST=$2

export S3_BUCKET=`echo $3 | sed 's/s3:\/\///' | sed 's/\/$//'`
export S3_UPLOAD_ROOT=`echo $4 | sed 's/s3:\/\///' | sed 's/\/$//'`
export S3_UPLOAD_PATH="$S3_UPLOAD_ROOT/$SM"
export LANES=${@:5:$#}

export HOME=/shared/home/ngs-user

#export JAVA_HOME=$HOME/opt/jdk1.6.0_27/
#export PATH=$HOME/bin:$HOME/opt/Python-2.7.2/bin:$HOME/opt/aria2-1.13.0/bin:$HOME/opt/apache-ant-1.8.2/bootstrap/bin:$HOME/opt/apache-ant-1.8.2/dist/bin:$HOME/opt/jdk1.6.0_27/bin:$HOME/opt/jdk1.6.0_27/db/bin:$HOME/opt/jdk1.6.0_27/jre/bin:$HOME/opt/git-1.7.7.1/bin:$HOME/opt/git-1.7.7.1/perl/blib/bin:$HOME/opt/bwa-0.5.9:$HOME/opt/samtools-0.1.17:$HOME/opt/jets3t-0.8.1/bin:$HOME/opt/R-2.13.1/bin/:$PATH

#export PATH=$HOME/bin:$HOME/opt/Python-2.7.2/bin:$HOME/opt/aria2-1.13.0/bin:$HOME/opt/apache-ant-1.8.2/bootstrap/bin:$HOME/opt/apache-ant-1.8.2/dist/bin:$HOME/opt/jdk1.6.0_27/bin:$HOME/opt/jdk1.6.0_27/db/bin:$HOME/opt/jdk1.6.0_27/jre/bin:$HOME/opt/git-1.7.7.1/bin:$HOME/opt/git-1.7.7.1/perl/blib/bin:$HOME/opt/bwa-0.5.9:$HOME/opt/samtools-0.1.17:/usr/kerberos/bin:/usr/local/bin:/bin:/usr/bin:/opt/condor/current/bin:/usr/java/default/bin:$PATH

export WORK=/mnt/scratch/$SM
export RESOURCES=$WORK/resources
export TMP=$WORK/tmp
export TMP_DATA=$WORK/TMP_DATA
export DATA=$WORK/data
export LISTS=$WORK/lists
export GATK="$HOME/opt/jdk1.6.0_27/bin/java -Djava.io.tmpdir=$TMP -jar $HOME/opt/GATK-Lilly/dist/GenomeAnalysisTK.jar"

export CPUS=`cat /proc/cpuinfo | grep -c processor`

export BAM_LIST="$LISTS/$SM.bam.list"
export MERGED_BAM="$WORK/aggregated.$ID.bam"


s3_bucket=`echo $S3_UPLOAD_PATH | sed "s/\/.*//"`

echo "Creating $WORK directory..."
#rm -rf $WORK
mkdir -p $WORK
mkdir -p $DATA
mkdir -p $TMP_DATA
mkdir -p $TMP
mkdir -p $LISTS

echo "Changing to $WORK directory..."
cd $WORK

echo "Extracting NGS resources..."

#python $HOME/bin/s3_down_file.py -b $S3_BUCKET -s resources/resources.tar.gz -d $RESOURCES -f resources.tar.gz

#gunzip -c $RESOURCES/resources.tar.gz | tar -C $RESOURCES -xf -

echo "Downloading lane BAMs..."
for LANE_BAM in $LANES
do
	LANE_BAI=`echo $LANE_BAM | sed 's/.bam/.bai/'`

	BAM_BASENAME=`basename $LANE_BAM`
	BAI_BASENAME=`basename $LANE_BAI`

    	echo "Downloading lane bam $LANE_BAM for sample $SM ..."

	s3_bam_file=`echo $LANE_BAM | sed 's/s3:\/\/[A-Za-z_\-]*\///'`
#	python $HOME/bin/s3_down_file.py -b $S3_BUCKET -s $s3_bam_file -d $TMP_DATA

	s3_bai_file=`echo $LANE_BAI | sed 's/s3:\/\/[A-Za-z_\-]*\///'`
#	python $HOME/bin/s3_down_file.py -b $S3_BUCKET -s $s3_bai_file -d $TMP_DATA

done
find $TMP_DATA -name \*.bam > $BAM_LIST

echo "Merging Lane level bam files, then split into chromosome files"

s3_path=`echo $S3_UPLOAD_PATH | sed 's/[A-Za-z_\-]*\///'`
s3_bucket=`echo $S3_UPLOAD_PATH | sed "s/\/.*//"`

IFS=":"
for CHR in $CHR_LIST
do

echo "CHR $CHR..."

    export OUT=$WORK/aggregation/temp/$CHR
    mkdir -p $OUT
    name=$SM.$CHR
    export BAM=$OUT/$name.pre_analysis.bam
    export BAI=$OUT/$name.pre_analysis.bai

    echo "$GATK -T PrintReads -R $RESOURCES/ucsc.hg19.fasta -L $CHR -I $BAM_LIST --disable_bam_indexing -o $BAM"
 
    echo "running GATK"

    #$GATK -T PrintReads -R $RESOURCES/ucsc.hg19.fasta -L $CHR -I $BAM_LIST --disable_bam_indexing -o $BAM

    $HOME/opt/jdk1.6.0_27/bin/java -Djava.io.tmpdir=$TMP -jar $HOME/opt/GATK-Lilly/dist/GenomeAnalysisTK.jar -T PrintReads -R $RESOURCES/ucsc.hg19.fasta -L $CHR -I $BAM_LIST --disable_bam_indexing -o $BAM

    echo "finished GATK for chr $CHR"

    $HOME/opt/samtools-1.1.17/samtools index $BAM.bai
    mv $BAM.bai $BAI

    echo "Uploading results..."
    echo "$s3_bucket $BAM $s3_path"
    echo "$s3_bucket $BAI $s3_path"

    python $HOME/bin/s3_upload_file.py $s3_bucket $BAM $s3_path
    python $HOME/bin/s3_upload_file.py $s3_bucket $BAI $s3_path

done
