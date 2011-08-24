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
import org.broadinstitute.sting.gatk.walkers.variantrecalibration.VariantRecalibratorArgumentCollection
import org.broadinstitute.sting.utils.variantcontext.VariantContext

class CancerCallingPipeline extends QScript {
  qscript =>

  @Input(doc="List of BAM files", fullName="input", shortName="i", required=true)
  var input: File = _

  @Input(doc="Reference fasta file", fullName="reference", shortName="R", required=true)
  var reference: File = _

  @Input(doc="Tumor samples", fullName="tumors", shortName="t", required=true)
  var tumorSamples: File = _

  @Input(doc="Normal samples", fullName="normal", shortName="n", required=true)
  var normalSamples: File = _

  @Input(doc="HapMap file", fullName="hapmap", shortName="H", required=true)
  var hapmap: File = _

  @Input(doc="OMNI file", fullName="omni", shortName="O", required=true)
  var omni: File = _

  @Input(doc="DbSNP file", fullName="dbsnp", shortName="D", required=true)
  var dbsnp: File = _

  @Input(doc="Mills-Devine indels file", fullName="mdindels", shortName="MD", required=true)
  var mdindels: File = _

  @Input(doc="The number of scatter-gather jobs to use", fullName="numJobs", shortName="j", required=false)
  var numJobs: Int = 1

  @Input(doc="the -L interval string to be used by GATK - output bams at interval only", fullName="gatk_interval_string", shortName="L", required=false)
  var intervalString: String = ""

  @Input(doc="an intervals file to be used by GATK - output bams at intervals only", fullName="gatk_interval_file", shortName="intervals", required=false)
  var intervals: File = _

  val queueLogDir: String = ".qlog/"  // Gracefully hide Queue's output

  trait CommandLineGATKArgs extends CommandLineGATK {
    this.memoryLimit = 4;
    this.reference_sequence = qscript.reference
    this.isIntermediate = false

    if (!qscript.intervalString.isEmpty()) this.intervalsString ++= List(qscript.intervalString)
    else if (qscript.intervals != null) this.intervals :+= qscript.intervals
  }

  case class callVariants(inBam: List[java.io.File], outVCF: File) extends UnifiedGenotyper with CommandLineGATKArgs {
    this.input_file = inBam
    this.rodBind :+= RodBind("dbsnp", "VCF", dbsnp)
    this.out = outVCF
    this.glm = org.broadinstitute.sting.gatk.walkers.genotyper.GenotypeLikelihoodsCalculationModel.Model.BOTH
    this.baq = org.broadinstitute.sting.utils.baq.BAQ.CalculationMode.OFF
    this.stand_call_conf = 20

    this.scatterCount = numJobs
    this.analysisName = queueLogDir + outVCF + ".callVariants"
    this.jobName = queueLogDir + outVCF + ".callVariants"
  }

  case class selectSamples(inVCF: File, inSamples: File, outVCF: File) extends SelectVariants with CommandLineGATKArgs {
    this.variantVCF = inVCF
    this.sample_file ++= List(inSamples)
    this.out = outVCF

    this.scatterCount = numJobs
    this.analysisName = queueLogDir + outVCF + ".selectVariants"
    this.jobName = queueLogDir + outVCF + ".selectVariants"
  }

  case class annotateVariants(inBam: List[java.io.File], inVCF: File, outVCF: File) extends VariantAnnotator with CommandLineGATKArgs {
    this.input_file = inBam
    this.variantVCF = inVCF
    this.out = outVCF
    this.A ++= List("FisherStrand")

    this.analysisName = queueLogDir + outVCF + ".annotateVariants"
    this.jobName = queueLogDir + outVCF + ".annotateVariants"
  }

  case class recalibrateSNPs(inVCF: File, outRscript: File, outTranches: File, outRecal: File) extends VariantRecalibrator with CommandLineGATKArgs {
    this.rodBind :+= RodBind("input", "VCF", inVCF)
    this.rodBind :+= RodBind("hapmap", "VCF", hapmap, "known=false,training=true,truth=true,prior=15.0")
    this.rodBind :+= RodBind("omni", "VCF", omni, "known=false,training=true,truth=true,prior=12.0")
    this.rodBind :+= RodBind("dbsnp", "VCF", dbsnp, "known=true,training=false,truth=false,prior=10.0")

    this.use_annotation ++= List("QD", "HaplotypeScore", "MQRankSum", "ReadPosRankSum", "FS", "MQ", "DP")
    this.allPoly = true
    this.mode = VariantRecalibratorArgumentCollection.Mode.SNP

    this.tranche ++= List("100.0", "99.9", "99.5", "99.3", "99.0", "98.9", "98.8", "98.5", "98.4", "98.3", "98.2", "98.1", "98.0", "97.9", "97.8", "97.5", "97.0", "95.0", "90.0")
    this.rscript_file = outRscript
    this.tranches_file = outTranches
    this.recal_file = outRecal

    this.analysisName = queueLogDir + outRecal + ".recalibrateSNPs"
    this.jobName =  queueLogDir + outRecal + ".recalibrateSNPs"
  }

  case class recalibrateIndels(inVCF: File, outRscript: File, outTranches: File, outRecal: File) extends VariantRecalibrator with CommandLineGATKArgs {
    this.rodBind :+= RodBind("input", "VCF", inVCF)
    this.rodBind :+= RodBind("training", "VCF", mdindels, "known=true,training=true,truth=true,prior=12.0")

    this.use_annotation ++= List("QD", "FS", "HaplotypeScore", "ReadPosRankSum")
    this.allPoly = true
    this.mode = VariantRecalibratorArgumentCollection.Mode.INDEL

    this.tranche ++= List("100.0", "99.9", "99.5", "99.3", "99.0", "98.9", "98.8", "98.5", "98.4", "98.3", "98.2", "98.1", "98.0", "97.9", "97.8", "97.5", "97.0", "95.0", "90.0")
    this.rscript_file = outRscript
    this.tranches_file = outTranches
    this.recal_file = outRecal

    this.analysisName = queueLogDir + outRecal + ".recalibrateIndels"
    this.jobName =  queueLogDir + outRecal + ".recalibrateIndels"
  }

  case class applyRecalibrationToSNPs(inVCF: File, inTranches: File, inRecal: File, outVCF: File) extends ApplyRecalibration with CommandLineGATKArgs {
    this.rodBind :+= RodBind("input", "VCF", inVCF)
    this.tranches_file = inTranches
    this.recal_file = inRecal
    this.ts_filter_level = 99.0
    this.mode = VariantRecalibratorArgumentCollection.Mode.SNP
    this.out = outVCF

    this.analysisName = queueLogDir + outVCF + ".applyRecalibrationToSNPs"
    this.jobName =  queueLogDir + outVCF + ".applyRecalibrationToSNPs"
  }

  case class applyRecalibrationToIndels(inVCF: File, inTranches: File, inRecal: File, outVCF: File) extends ApplyRecalibration with CommandLineGATKArgs {
    this.rodBind :+= RodBind("input", "VCF", inVCF)
    this.tranches_file = inTranches
    this.recal_file = inRecal
    this.ts_filter_level = 99.0
    this.mode = VariantRecalibratorArgumentCollection.Mode.INDEL
    this.out = outVCF

    this.analysisName = queueLogDir + outVCF + ".applyRecalibrationToIndels"
    this.jobName =  queueLogDir + outVCF + ".applyRecalibrationToIndels"
  }

  case class evaluateSNPs(inVCF: File, outEval: File) extends VariantEval with CommandLineGATKArgs {
    this.rodBind :+= RodBind("eval", "VCF", inVCF)
    this.rodBind :+= RodBind("dbsnp", "VCF", dbsnp)
    this.VT = List(VariantContext.Type.SNP)
    this.out = outEval
  }

  case class evaluateIndels(inVCF: File, outEval: File) extends VariantEval with CommandLineGATKArgs {
    this.rodBind :+= RodBind("eval", "VCF", inVCF)
    this.rodBind :+= RodBind("dbsnp", "VCF", dbsnp)
    this.VT = List(VariantContext.Type.INDEL)
    this.out = outEval
  }

  /****************************************************************************
  * Main script
  ****************************************************************************/

  def script = {
    val bams = QScriptUtils.createListFromFile(input)

    // SNPs
    val rawVariants                = "intermediate/variants/raw.vcf"

    // Tumor-specific files
    val tumorRawVariants           = "intermediate/variants/tumor/raw.vcf"
    val tumorRawAnnotatedVariants  = "intermediate/variants/tumor/raw.annotated.vcf"

    val tumorRscriptSNPs           = "intermediate/variants/tumor/snps.vqsr.R"
    val tumorTranchesSNPs          = "intermediate/variants/tumor/snps.tranches"
    val tumorRecalSNPs             = "intermediate/variants/tumor/snps.recal"

    val tumorRscriptIndels         = "intermediate/variants/tumor/indels.vqsr.R"
    val tumorTranchesIndels        = "intermediate/variants/tumor/indels.tranches"
    val tumorRecalIndels           = "intermediate/variants/tumor/indels.recal"

    val tumorRecalibratedSNPs      = "intermediate/variants/tumor/partially_recalibrated.vcf"

    val tumorRecalibratedVariants  = "tumor.analysis_ready.vcf"
    val tumorEvalSNPs              = "tumor.analysis_ready.snps.eval"
    val tumorEvalIndels            = "tumor.analysis_ready.indels.eval"

    // Normal-specific files
    val normalRawVariants          = "intermediate/variants/normal/raw.vcf"
    val normalRawAnnotatedVariants = "intermediate/variants/normal/raw.annotated.vcf"

    val normalRscriptSNPs          = "intermediate/variants/normal/snps.vqsr.R"
    val normalTranchesSNPs         = "intermediate/variants/normal/snps.tranches"
    val normalRecalSNPs            = "intermediate/variants/normal/snps.recal"

    val normalRscriptIndels        = "intermediate/variants/normal/indels.vqsr.R"
    val normalTranchesIndels       = "intermediate/variants/normal/indels.tranches"
    val normalRecalIndels          = "intermediate/variants/normal/indels.recal"

    val normalRecalibratedSNPs     = "intermediate/variants/normal/partially_recalibrated.vcf"

    val normalRecalibratedVariants = "normal.analysis_ready.vcf"
    val normalEvalSNPs             = "normal.analysis_ready.snps.eval"
    val normalEvalIndels           = "normal.analysis_ready.indels.eval"

    add(
      callVariants(bams, rawVariants),

      selectSamples(rawVariants, tumorSamples, tumorRawVariants),
      annotateVariants(bams, tumorRawVariants, tumorRawAnnotatedVariants),
      recalibrateSNPs(tumorRawAnnotatedVariants, tumorRscriptSNPs, tumorTranchesSNPs, tumorRecalSNPs),
      recalibrateIndels(tumorRawAnnotatedVariants, tumorRscriptIndels, tumorTranchesIndels, tumorRecalIndels),
      applyRecalibrationToSNPs(tumorRawAnnotatedVariants, tumorTranchesSNPs, tumorRecalSNPs, tumorRecalibratedSNPs),
      applyRecalibrationToIndels(tumorRecalibratedSNPs, tumorTranchesIndels, tumorRecalIndels, tumorRecalibratedVariants),
      evaluateSNPs(tumorRecalibratedVariants, tumorEvalSNPs),
      evaluateIndels(tumorRecalibratedVariants, tumorEvalIndels),

      selectSamples(rawVariants, normalSamples, normalRawVariants),
      annotateVariants(bams, normalRawVariants, normalRawAnnotatedVariants),
      recalibrateSNPs(normalRawAnnotatedVariants, normalRscriptSNPs, normalTranchesSNPs, normalRecalSNPs),
      recalibrateIndels(normalRawAnnotatedVariants, normalRscriptIndels, normalTranchesIndels, normalRecalIndels),
      applyRecalibrationToSNPs(normalRawAnnotatedVariants, normalTranchesSNPs, normalRecalSNPs, normalRecalibratedSNPs),
      applyRecalibrationToIndels(normalRecalibratedSNPs, normalTranchesIndels, normalRecalIndels, normalRecalibratedVariants),
      evaluateSNPs(normalRecalibratedVariants, normalEvalSNPs),
      evaluateIndels(normalRecalibratedVariants, normalEvalIndels)
    )
  }
}
