#!/bin/bash

#$ -N LaneProcessingPipeline
#$ -S /bin/bash
#$ -cwd
#$ -j y
#$ -m be

#set -x

export SM=$1
export CHR=$2
export ID="$1.$2"
# johny is not using $S3_BUCKET
export S3_BUCKET=`echo $3 | sed 's/s3:\/\///' | sed 's/\/$//'`
export S3_UPLOAD_ROOT=`echo $4 | sed 's/s3:\/\///' | sed 's/\/$//'`
export S3_UPLOAD_PATH="$S3_UPLOAD_ROOT/$SM"
# 5th arg is an array of lanebams. that is below
export LANES=${@:5:$#}

#Johny:  Set HOME variable to the directory where you installed the tools. Set WORK variable to the /lrlhps/scratch/userid/$SM/$CHR/aggregation
#These are the only two change in this file.
# Note : There should not be space around the "=" sign  ; bash creates problem

#export HOME=/shared/home/lilly-collab
export HOME=/lrlhps/users/c085541/GATK

export JAVA_HOME=$HOME/opt/jdk1.6.0_27/
export PATH=$HOME/opt/apache-ant-1.8.2/bootstrap/bin:$HOME/opt/apache-ant-1.8.2/dist/bin:$HOME/opt/jdk1.6.0_27/bin:$HOME/opt/jdk1.6.0_27/db/bin:$HOME/opt/jdk1.6.0_27/jre/bin:$HOME/opt/git-1.7.6/bin:$HOME/opt/git-1.7.6/perl/blib/bin:$HOME/opt/bwa-0.5.9:$HOME/opt/samtools-0.1.17:$HOME/opt/jets3t-0.8.1/bin:$HOME/opt/R-2.13.1/bin/:$PATH

export WORK=/lrlhps/scratch/c085541/ACRG/work/$SM/$CHR/aggregation

export RESOURCES=$WORK/resources
export TMP=$WORK/tmp
export DATA=$WORK/data
export LISTS=$WORK/lists

export GATK="java -Djava.io.tmpdir=$TMP -jar $HOME/opt/GATK-Lilly/dist/GenomeAnalysisTK.jar";
export QUEUE="java -Djava.io.tmpdir=$TMP -jar $HOME/opt/GATK-Lilly/dist/Queue.jar";
export CPUS=`cat /proc/cpuinfo | grep -c processor`

export BAM=$ID.analysis_ready.bam
export BAI=$ID.analysis_ready.bai
export BAM_LIST="$LISTS/$SM.bam.list"
export MERGED_BAM="$WORK/aggregated.$ID.bam"

#Johny added 
#Create output upload path S3_UPLOAD_PATH
if [ ! -e $S3_UPLOAD_PATH ]; then
	echo "Creating $S3_UPLOAD_PATH directory..."
	mkdir -p $S3_UPLOAD_PATH
fi

echo "Creating $WORK directory..."
rm -rf $WORK
mkdir -p $WORK
mkdir $DATA
mkdir $TMP
mkdir $LISTS

echo "Changing to $WORK directory..."
cd $WORK

echo "Extracting NGS resources..."
# s3cmd sync s3://$S3_BUCKET/resources $WORK/
# gunzip -c $RESOURCES/resources.tar.gz | tar -C $RESOURCES -xf -
# new logic is to create a soft link
ln -s /lrlhps/data/genomics/ref/gatk resources

echo "Downloading lane BAMs..."
for LANE_BAM in $LANES
do
	LANE_BAI=`echo $LANE_BAM | sed 's/.bam/.bai/'`

	BAM_BASENAME=`basename $LANE_BAM`
	BAI_BASENAME=`basename $LANE_BAI`
echo $DATA/$BAM_BASENAME
echo $LANE_BAM
# 	s3cmd sync $LANE_BAM $DATA/$BAM_BASENAME
# 	s3cmd sync $LANE_BAI $DATA/$BAI_BASENAME
	ln -s $LANE_BAM $DATA/$BAM_BASENAME
	ln -s $LANE_BAI $DATA/$BAI_BASENAME
done
find $DATA -name \*.bam > $BAM_LIST

echo "Merging BAM files, ensuring uniquified read names..."
$GATK -T PrintReads -R $RESOURCES/ucsc.hg19.fasta -L $CHR -I $BAM_LIST -addrg --disable_bam_indexing -o $MERGED_BAM
$HOME/opt/samtools-0.1.17/samtools index $MERGED_BAM

echo "Running sample-level pipeline..."
$QUEUE -S $HOME/opt/GATK-Lilly/public/scala/qscript/org/broadinstitute/sting/queue/qscripts/DataProcessingPipeline.scala -i $MERGED_BAM -r $HOME/opt/GATK-Lilly/public/R -R $RESOURCES/ucsc.hg19.fasta -D $RESOURCES/dbsnp_132.hg19.vcf -indels $RESOURCES/1000G_indels_for_realignment.hg19.vcf -L $CHR -nv -p processed -run
ln -s processed.$SM.clean.dedup.recal.bam $BAM
ln -s processed.$SM.clean.dedup.recal.bai $BAI
tar -cf processed.$ID.pre.tar processed.$SM.pre/ && gzip processed.$ID.pre.tar
tar -cf processed.$ID.post.tar processed.$SM.post/ && gzip processed.$ID.post.tar

echo "Uploading results..."

mv $BAM $S3_UPLOAD_PATH
mv $BAI $S3_UPLOAD_PATH 
mv processed.$ID.pre.tar.gz $S3_UPLOAD_PATH
mv processed.$ID.post.tar.gz $S3_UPLOAD_PATH

# bamSrcSize=$(stat -c %s $BAM)
# bamDestSize=$(stat -c %s $S3_UPLOAD_PATH.$BAM)
# if [$bamSrcSize = $bamDestSize  ]; then
# 	#rm $BAM;
# 	echo $BAM."=".$bamSrcSize
# fi

# baiSrcSize=$(stat -c %s $BAI)
# baiDestSize=$(stat -c %s $S3_UPLOAD_PATH.$BAI)
# if [$baiSrcSize = $baiDestSize  ]; then
# 	#rm $BAM;
# 	echo $BAI."=".$baiSrcSize
# fi

echo "Done."
