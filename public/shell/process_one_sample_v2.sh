#!/bin/bash

export SM=$1
export CHR=$2
export ID="$1.$2"
export S3_BUCKET=`echo $3 | sed 's/s3:\/\///' | sed 's/\/$//'`
export S3_CHR_PATH=`echo $4 | sed 's/s3:\/\///' | sed 's/\/$//'`
#s3_bam_file=`echo $LANE_BAM | sed 's/s3:\/\/[A-Za-z_\-]*\///'`

export CHR_BAM="$S3_CHR_PATH/$SM/$ID.pre_analysis.bam"
export CHR_BAI=`echo $CHR_BAM | sed 's/.bam/.bai/'`

export S3_UPLOAD_ROOT=`echo $5 | sed 's/s3:\/\///' | sed 's/\/$//'`
export S3_UPLOAD_PATH="$S3_UPLOAD_ROOT/$SM"


export HOME=/shared/home/ngs-user
export JAVA_HOME=$HOME/opt/jdk1.6.0_27/
export PATH=$HOME/bin:$HOME/opt/Python-2.7.2/bin:$HOME/opt/aria2-1.13.0/bin:$HOME/opt/apache-ant-1.8.2/bootstrap/bin:$HOME/opt/apache-ant-1.8.2/dist/bin:$HOME/opt/jdk1.6.0_27/bin:$HOME/opt/jdk1.6.0_27/db/bin:$HOME/opt/jdk1.6.0_27/jre/bin:$HOME/opt/git-1.7.7.1/bin:$HOME/opt/git-1.7.7.1/perl/blib/bin:$HOME/opt/bwa-0.5.9:$HOME/opt/samtools-0.1.17:$HOME/opt/jets3t-0.8.1/bin:$HOME/opt/R-2.13.1/bin/:$PATH

export WORK=/mnt/scratch/$SM/$CHR/aggregation
export RESOURCES=$WORK/resources
export TMP=$WORK/tmp
export DATA=$WORK/data


export GATK="java -Djava.io.tmpdir=$TMP -jar $HOME/opt/GATK-Lilly/dist/GenomeAnalysisTK.jar";
export QUEUE="java -Djava.io.tmpdir=$TMP -jar $HOME/opt/GATK-Lilly/dist/Queue.jar";
export CPUS=`cat /proc/cpuinfo | grep -c processor`

export BAM=$ID.analysis_ready.bam
export BAI=$ID.analysis_ready.bai

#export MERGED_BAM="$WORK/aggregated.$ID.bam"

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
gunzip -c $RESOURCES/resources.tar.gz | tar -C $RESOURCES -xf -

echo "Downloading CHR BAM..."

echo "$HOME/bin/s3_down_file.py -b $S3_BUCKET -s $CHR_BAM -d $DATA"
python $HOME/bin/s3_down_file.py -b $S3_BUCKET -s $CHR_BAM -d $DATA
python $HOME/bin/s3_down_file.py -b $S3_BUCKET -s $CHR_BAI -d $DATA


echo "Running sample-level pipeline..."
$QUEUE -S $HOME/opt/GATK-Lilly/public/scala/qscript/org/broadinstitute/sting/queue/qscripts/DataProcessingPipeline.scala -i $CHR_BAM -r $HOME/opt/GATK-Lilly/public/R -R $RESOURCES/ucsc.hg19.fasta -D $RESOURCES/dbsnp_132.hg19.vcf -indels $RESOURCES/1000G_indels_for_realignment.hg19.vcf -L $CHR -nv -p processed -run
ln -s processed.$SM.clean.dedup.recal.bam $BAM
ln -s processed.$SM.clean.dedup.recal.bai $BAI
tar -cf processed.$ID.pre.tar processed.$SM.pre/ && gzip processed.$ID.pre.tar
tar -cf processed.$ID.post.tar processed.$SM.post/ && gzip processed.$ID.post.tar

echo "Uploading results..."

s3_path=`echo $S3_UPLOAD_PATH | sed 's/[A-Za-z_\-]*\///'`
s3_bucket=`echo $S3_UPLOAD_PATH | sed "s/\/.*//"`

echo "$s3_bucket $BAM $s3_path"
echo $BAI

python $HOME/bin/s3_upload_file.py $s3_bucket $BAM $s3_path
python $HOME/bin/s3_upload_file.py $s3_bucket $BAI $s3_path
python $HOME/bin/s3_upload_file.py $s3_bucket processed.$ID.pre.tar.gz $s3_path
python $HOME/bin/s3_upload_file.py $s3_bucket processed.$ID.post.tar.gz $s3_path

bamSrcSize=$(stat -c %s $BAM)
bamDestSize=$(s3cmd ls $S3_UPLOAD_PATH$BAM|awk '{print $3}')
echo " $S3_UPLOAD_PATH$BAM"

bamSrcSize=$(stat -L -c %s $BAM)
bamDestSize=$(s3cmd ls 's3://'$S3_UPLOAD_PATH'/'$BAM|awk '{print $3}')

baiSrcSize=$(stat -L -c %s $BAI)
baiDestSize=$(s3cmd ls 's3://'$S3_UPLOAD_PATH'/'$BAI|awk '{print $3}')

echo $BAM"="$bamSrcSize
echo "Dest"$BAM"="$bamDestSize

echo $BAI"="$baiSrcSize
echo "Dest " $BAI"="$baiDestSize

if [ "$bamSrcSize" == "$bamDestSize" -a "$baiSrcSize" == "$baiDestSize" ]; then
        rm -rf $WORK
        echo "Removed "$WORK
else
    echo "Uploading to S3 does not result in matching bam and bai file sizes"
fi

echo "Done."
