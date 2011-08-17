#!/bin/bash

mkdir resources

wget gsapubftp-anonymous@ftp.broadinstitute.org:/bundle/1.1/hg19/ucsc.hg19.fasta.gz -O resources/ucsc.hg19.fasta.gz
wget gsapubftp-anonymous@ftp.broadinstitute.org:/bundle/1.1/hg19/ucsc.hg19.fasta.fai.gz -O resources/ucsc.hg19.fasta.fai.gz
wget gsapubftp-anonymous@ftp.broadinstitute.org:/bundle/1.1/hg19/ucsc.hg19.dict.gz -O resources/ucsc.hg19.dict.gz

wget gsapubftp-anonymous@ftp.broadinstitute.org:/bundle/1.1/hg19/dbsnp_132.hg19.vcf.gz -O resources/dbsnp_132.hg19.vcf.gz
wget gsapubftp-anonymous@ftp.broadinstitute.org:/bundle/1.1/hg19/dbsnp_132.hg19.vcf.idx.gz -O resources/dbsnp_132.hg19.vcf.idx.gz

wget gsapubftp-anonymous@ftp.broadinstitute.org:/bundle/1.1/hg19/dbsnp_132.hg19.excluding_sites_after_129.vcf.gz -O resources/dbsnp_132.hg19.excluding_sites_after_129.vcf.gz
wget gsapubftp-anonymous@ftp.broadinstitute.org:/bundle/1.1/hg19/dbsnp_132.hg19.excluding_sites_after_129.vcf.idx.gz -O resources/dbsnp_132.hg19.excluding_sites_after_129.vcf.idx.gz

wget gsapubftp-anonymous@ftp.broadinstitute.org:/bundle/1.1/hg19/1000G_indels_for_realignment.hg19.vcf.gz -O resources/1000G_indels_for_realignment.hg19.vcf.gz
wget gsapubftp-anonymous@ftp.broadinstitute.org:/bundle/1.1/hg19/1000G_indels_for_realignment.hg19.vcf.idx.gz -O resources/1000G_indels_for_realignment.hg19.vcf.idx.gz

gunzip -f resources/*
