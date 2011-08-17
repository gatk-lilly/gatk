package org.broadinstitute.sting.queue.qscripts

import org.broadinstitute.sting.queue.extensions.gatk._
import org.broadinstitute.sting.queue.QScript
import org.broadinstitute.sting.queue.extensions.picard._
import org.broadinstitute.sting.gatk.walkers.indels.IndelRealigner.ConsensusDeterminationModel
import org.broadinstitute.sting.utils.baq.BAQ.CalculationMode

import collection.JavaConversions._
import net.sf.samtools.SAMFileReader
import net.sf.samtools.SAMFileHeader.SortOrder

import org.broadinstitute.sting.queue.util.QScriptUtils
import org.broadinstitute.sting.queue.function.{CommandLineFunction, ListWriterFunction}

class DataProcessingPipeline extends QScript {
  qscript =>

  @Input(doc="input fastq file for paired end1", fullName="fastq1", shortName="f1", required=true)
  var fastq1: File = _

  @Input(doc="input fastq file for paired end2", fullName="fastq2", shortName="f2", required=true)
  var fastq2: File = _

  @Input(doc="Reference fasta file", fullName="reference", shortName="R", required=true)
  var reference: File = _

  @Input(doc="The path to the binary of bwa", fullName="path_to_bwa", shortName="bwa", required=true)
  var bwaPath: File = _

  @Input(doc="The name of the final BAM", fullName="bamName", shortName="name", required=true)
  var bamName: String = _

  @Input(doc="The read group text to place in the BAM, without the extension", fullName="read_group", shortName="rg", required=true)
  var readGroupText: String = _

  val queueLogDir: String = ".qlog/"  // Gracefully hide Queue's output

  // General arguments to non-GATK tools
  trait ExternalCommonArgs extends CommandLineFunction {
    this.memoryLimit = 4
    this.isIntermediate = true
  }

  case class bwa_aln_fastq (inFastq: File, outSai: File) extends CommandLineFunction with ExternalCommonArgs {
    @Input(doc="fastq file to be aligned") var fastq = inFastq
    @Output(doc="output sai file for pair end") var sai = outSai

    def commandLine = bwaPath + " aln -t 1 -q 5 " + reference + " " + fastq + " > " + sai

    this.analysisName = queueLogDir + outSai + ".bwa_aln_fastq"
    this.jobName = queueLogDir + outSai + ".bwa_aln_fastq"
  }

  case class bwa_sam_pe_fastq(inFastq1: File, inFastq2: File, inSai1: File, inSai2:File, outBam: File) extends CommandLineFunction with ExternalCommonArgs {
    @Input(doc="fastq file for first end to be aligned") var fastq1 = inFastq1
    @Input(doc="fastq file for second end to be aligned") var fastq2 = inFastq2
    @Input(doc="bwa alignment index file for 1st mating pair") var sai1 = inSai1
    @Input(doc="bwa alignment index file for 2nd mating pair") var sai2 = inSai2
    @Output(doc="output aligned bam file") var alignedBam = outBam

    def commandLine = bwaPath + " sampe -r \'" + readGroupText + "\' " + reference + " " + sai1 + " " + sai2 + " " + fastq1 + " " + fastq2 + " > " + alignedBam

    this.memoryLimit = 2
    this.analysisName = queueLogDir + outBam + ".bwa_sam_pe"
    this.jobName = queueLogDir + outBam + ".bwa_sam_pe"
    this.isIntermediate = false
  }

  /****************************************************************************
  * Main script
  ****************************************************************************/

  def script = {
  	var saiFile1 = "intermediate/end1.sai";
  	var saiFile2 = "intermediate/end2.sai";
	var bamFile = bamName;

	add(bwa_aln_fastq(fastq1, saiFile1),
	    bwa_aln_fastq(fastq2, saiFile2),
	    bwa_sam_pe_fastq(fastq1, fastq2, saiFile1, saiFile2, bamFile)
	)
  }
}
