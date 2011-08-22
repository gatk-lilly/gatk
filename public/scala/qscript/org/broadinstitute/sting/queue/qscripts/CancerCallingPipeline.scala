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

class CancerCallingPipeline extends QScript {
  qscript =>

  @Input(doc="input BAM file - or list of BAM files", fullName="input", shortName="i", required=true)
  var input: File = _

  @Input(doc="Reference fasta file", fullName="reference", shortName="R", required=true)
  var reference: File = _

  @Input(doc="DbSNP file", fullName="dbsnp", shortName="D", required=true)
  var dbsnp: File = _

  @Input(doc="The number of scatter-gather jobs to use", fullName="numJobs", shortName="j", required=false)
  var numJobs: Int = 1

  val queueLogDir: String = ".qlog/"  // Gracefully hide Queue's output

  trait CommandLineGATKArgs extends CommandLineGATK with ExternalCommonArgs {
    this.memoryLimit = 4;
    this.reference_sequence = qscript.reference
    this.isIntermediate = false
  }

  case class callSNPs(inBam: File, outVcf: File) extends UnifiedGenotyper with CommandLineGATKArgs {
    this.input_file :+= inBam
    this.rodBind :+= RodBind("dbsnp", "VCF", dbsnp)
    this.out = outVcf
    this.glm = org.broadinstitute.sting.gatk.walkers.genotyper.GenotypeLikelihoodsCalculationModel.Model.SNP
    this.baq = org.broadinstitute.sting.utils.baq.BAQ.CalculationMode.CALCULATE_AS_NECESSARY
    this.A ++= List("FisherStrand", "FS", "InbreedingCoeff")

    this.scatterCount = numJobs
    this.analysisName = queueLogDir + outVcf + ".callSNPs"
    this.jobName = queueLogDir + outVcf + ".callSNPs"
  }

  case class callIndels(inBam: File, outVcf: File) extends UnifiedGenotyper with CommandLineGATKArgs {
    this.input_file :+= inBam
    this.rodBind :+= RodBind("dbsnp", "VCF", dbsnp)
    this.out = outVcf
    this.glm = org.broadinstitute.sting.gatk.walkers.genotyper.GenotypeLikelihoodsCalculationModel.Model.INDEL
    this.baq = org.broadinstitute.sting.utils.baq.BAQ.CalculationMode.OFF

    this.scatterCount = numJobs
    this.analysisName = queueLogDir + outVcf + ".callIndels"
    this.jobName = queueLogDir + outVcf + ".callIndels"
  }

  case class filterIndels(inVCF: File, outVCF: File) extends VariantFiltration with CommandLineGATKArgs {
    this.variantVCF = inVCF
    this.memoryLimit = 2
    this.filterName ++= List("HardToValidate", "LowQual", "StrandBias", "QualByDepth", "HomopolymerRun")
    this.filterExpression ++= List("\"MQ0 >= 4 && (MQ0 / (1.0 * DP)) > 0.1\"", "\"QUAL<50.0\"", "\"SB>=-1.0\"", "\"QD<5.0\"", "\"HRun>=15\"")
    this.out = outVCF

    this.scatterCount = 10
    this.analysisName = queueLogDir + outVCF + ".filterIndels"
    this.jobName = queueLogDir + outVCF + ".filterIndels"
  }

  case class VQSR(inVCF: File, outTranches: File, outRecal: File) extends VariantRecalibrator with CommandLineGATKArgs {
    this.rodBind :+= RodBind("input", "VCF", inVCF )
    this.rodBind :+= RodBind("hapmap", "VCF", hapmapFile, "known=false,training=true,truth=true,prior=15.0")
    this.rodBind :+= RodBind("omni", "VCF", omni_b37, "known=false,training=true,truth=true,prior=12.0")
    this.rodBind :+= RodBind("dbsnp", "VCF", dbsnpFile, "known=true,training=false,truth=false,prior=10.0")
    this.use_annotation ++= List("QD", "HaplotypeScore", "MQRankSum", "ReadPosRankSum", "HRun", "FS")
    this.allPoly = true
    this.tranche ++= List("100.0", "99.9", "99.5", "99.3", "99.0", "98.9", "98.8", "98.5", "98.4", "98.3", "98.2", "98.1", "98.0", "97.9", "97.8", "97.5", "97.0", "95.0", "90.0")
    this.rscript_file = vqsrRscript
    this.tranches_file = tranchesFile
    this.recal_file = recalFile

    this.analysisName = queueLogDir + outVCF + ".VQSR"
    this.jobName =  queueLogDir + outVCF + ".VQSR"
  }

  case class applyVQSR (inVCF: File, inTranches: File, inRecal: File, outVCF: File) extends ApplyRecalibration with CommandLineGATKArgs {
    this.rodBind :+= RodBind("input", "VCF", inVCF)
    this.tranches_file = inTranches
    this.recal_file = inRecal
    this.ts_filter_level = trancheTarget
    this.out = outVCF

    this.analysisName = queueLogDir + outVCF + ".applyVQSR"
    this.jobName =  queueLogDir + outVCF + ".applyVQSR"
  }

  /****************************************************************************
  * Main script
  ****************************************************************************/

  def script = {
	val bams = QScriptUtils.createListFromFile(input)

  	var rawSNPs = "intermediate/snps/snps.raw.vcf"
	var tranches = "intermediate/snps/snps.tranches"
	var recal = "intermediate/snps/snps.recal"
	var recalibratedSNPs = "snps.analysis_ready.vcf"

  	var rawIndels = "intermediate/indels.raw.vcf"
	var filteredIndels = "indels.analysis_ready.vcf"

	add(callSNPs(bams, rawSNPs),
	    VQSR(rawSNPs, tranches, recal),
	    applyVQSR(rawSNPs, tranches, recal, recalibratedSNPs),

	    callIndels(bams, rawIndels),
	    filterIndels(rawIndels, filteredIndels)
	)
  }
}
