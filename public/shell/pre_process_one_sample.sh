#!/bin/bash

export SM=$1
export CHR_LIST=$2
#export ID="$1.$2"
export S3_BUCKET=`echo $3 | sed 's/s3:\/\///' | sed 's/\/$//'`
export S3_UPLOAD_ROOT=`echo $4 | sed 's/s3:\/\///' | sed 's/\/$//'`
export S3_UPLOAD_PATH="$S3_UPLOAD_ROOT/$SM"
export LANES=${@:5:$#}

export HOME=/shared/home/ngs-user
export JAVA_HOME=$HOME/opt/jdk1.6.0_27/
export PATH=$HOME/bin:$HOME/opt/Python-2.7.2/bin:$HOME/opt/aria2-1.13.0/bin:$HOME/opt/apache-ant-1.8.2/bootstrap/bin:$HOME/opt/apache-ant-1.8.2/dist/bin:$HOME/opt/jdk1.6.0_27/bin:$HOME/opt/jdk1.6.0_27/db/bin:$HOME/opt/jdk1.6.0_27/jre/bin:$HOME/opt/git-1.7.7.1/bin:$HOME/opt/git-1.7.7.1/perl/blib/bin:$HOME/opt/bwa-0.5.9:$HOME/opt/samtools-0.1.17:$HOME/opt/jets3t-0.8.1/bin:$HOME/opt/R-2.13.1/bin/:$PATH

#export WORK=/mnt/scratch/$SM/$CHR/aggregation
export WORK=/mnt/scratch/$SM

export RESOURCES=$WORK/resources
export TMP=$WORK/tmp
export TMP_DATA=$WORK/TMP_DATA
export DATA=$WORK/data
export LISTS=$WORK/lists

export GATK="java -Djava.io.tmpdir=$TMP -jar $HOME/opt/GATK-Lilly/dist/GenomeAnalysisTK.jar";

export CPUS=`cat /proc/cpuinfo | grep -c processor`

#export BAM=$ID.analysis_ready.bam
#export BAI=$ID.analysis_ready.bai

export BAM_LIST="$LISTS/$SM.bam.list"
export MERGED_BAM="$WORK/aggregated.$ID.bam"


s3_bucket=`echo $S3_UPLOAD_PATH | sed "s/\/.*//"`

echo "Creating $WORK directory..."
rm -rf $WORK
mkdir -p $WORK
mkdir $DATA
mkdir $TMP
mkdir $LISTS

echo "Changing to $WORK directory..."
cd $WORK

echo "Extracting NGS resources..."

python $HOME/bin/s3_down_file.py -b $S3_BUCKET -s resources/resources.tar.gz -d $RESOURCES -f resources.tar.gz
python $HOME/bin/s3_down_file.py -b $S3_BUCKET -s resources/variant_calling_resources.tar.gz  -d $RESOURCES -f variant_calling_resources.tar.gz

gunzip -c $RESOURCES/resources.tar.gz | tar -C $RESOURCES -xf -

echo "Downloading lane BAMs..."
for LANE_BAM in $LANES
do
	LANE_BAI=`echo $LANE_BAM | sed 's/.bam/.bai/'`

	BAM_BASENAME=`basename $LANE_BAM`
	BAI_BASENAME=`basename $LANE_BAI`

    s3_bam_file=`echo $LANE_BAM | sed 's/s3:\/\/[A-Za-z_\-]*\///'`
    python $HOME/bin/s3_down_file.py -b $S3_BUCKET -s $s3_bam_file -d $DATA
        
    s3_bai_file=`echo $LANE_BAI | sed 's/s3:\/\/[A-Za-z_\-]*\///'`
    python $HOME/bin/s3_down_file.py -b $S3_BUCKET -s $s3_bai_file -d $DATA

	#fix the readname here in the current set, move this up to step one in the future
    $GATK -T PrintReads -R $RESOURCES/ucsc.hg19.fasta -I $TMP_DATA/$BAM_BASENAME -addrg --disable_bam_indexing -o $DATA/$BAM_BASENAME
    $HOME/opt/samtools-0.1.17/samtools index $DATA/$BAM_BASENAME

done
find $DATA -name \*.bam > $BAM_LIST

#echo "Merging BAM files, ensuring uniquified read names..."
echo "Merging Lane level bam files, then split into chromosome files"
#$GATK -T PrintReads -R $RESOURCES/ucsc.hg19.fasta -L $CHR -I $BAM_LIST -addrg --disable_bam_indexing -o $MERGED_BAM
#$HOME/opt/samtools-0.1.17/samtools index $MERGED_BAM

s3_path=`echo $S3_UPLOAD_PATH | sed 's/[A-Za-z_\-]*\///'`
s3_bucket=`echo $S3_UPLOAD_PATH | sed "s/\/.*//"`

IFS=":"
for CHR in $CHR_LIST
do
    export $OUT=$WORK/$CHR/aggregation
    mkdir -p $OUT
    name=$SM.$CHR
    BAM=$OUT/$name.pre_analysis.bam
    BAI=$OUT/$name.pre_analysis.bai
    $GATK -T PrintReads -R $RESOURCES/ucsc.hg19.fasta -L $CHR -I $BAM_LIST --disable_bam_indexing -o $BAM
    $HOME/opt/samtools-0.1.17/samtools index $BAM

    echo "Uploading results..."
    echo "$s3_bucket $BAM $s3_path"
    echo "$s3_bucket $BAI $s3_path"

    python $HOME/bin/s3_upload_file.py $s3_bucket $BAM $s3_path
    python $HOME/bin/s3_upload_file.py $s3_bucket $BAI $s3_path

done

