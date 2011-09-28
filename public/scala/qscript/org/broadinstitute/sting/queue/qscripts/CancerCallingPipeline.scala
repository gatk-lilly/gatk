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

  @Input(doc="List of tumor BAM files", fullName="tumorbams", shortName="tb", required=true)
  var tumorBams: File = _

  @Input(doc="List of normal BAM files", fullName="normalbams", shortName="nb", required=true)
  var normalBams: File = _

  @Input(doc="Tumor samples", fullName="tumors", shortName="t", required=true)
  var tumorSamples: File = _

  @Input(doc="Normal samples", fullName="normal", shortName="n", required=true)
  var normalSamples: File = _

  @Input(doc="Reference fasta file", fullName="reference", shortName="R", required=true)
  var reference: File = _

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

  case class callVariants(inBam: List[java.io.File], outVCF: File, dbSNP: File) extends UnifiedGenotyper with CommandLineGATKArgs {
    this.input_file = inBam
    this.D = dbSNP
    this.out = outVCF
    this.glm = org.broadinstitute.sting.gatk.walkers.genotyper.GenotypeLikelihoodsCalculationModel.Model.BOTH
    this.baq = org.broadinstitute.sting.utils.baq.BAQ.CalculationMode.OFF
    this.stand_call_conf = 20

    this.scatterCount = numJobs
    this.analysisName = queueLogDir + outVCF + ".callVariants"
    this.jobName = queueLogDir + outVCF + ".callVariants"
  }

  case class selectSamples(inVCF: File, inSamples: File, outVCF: File) extends SelectVariants with CommandLineGATKArgs {
    this.variant = inVCF
    this.sample_file ++= List(inSamples)
    this.out = outVCF
    this.excludeNonVariants = true

    this.analysisName = queueLogDir + outVCF + ".selectVariants"
    this.jobName = queueLogDir + outVCF + ".selectVariants"
  }

  case class selectSNPs(inVCF: File, outVCF: File) extends SelectVariants with CommandLineGATKArgs {
    this.variant = inVCF
    this.out = outVCF
    //this.snps = true
    this.selectTypeToInclude = List(VariantContext.Type.SNP)

    this.analysisName = queueLogDir + outVCF + ".selectSNPs"
    this.jobName = queueLogDir + outVCF + ".selectSNPs"
  }

  case class selectIndels(inVCF: File, outVCF: File) extends SelectVariants with CommandLineGATKArgs {
    this.variant = inVCF
    this.out = outVCF
    //this.indels = true
    this.selectTypeToInclude = List(VariantContext.Type.INDEL)

    this.analysisName = queueLogDir + outVCF + ".selectIndels"
    this.jobName = queueLogDir + outVCF + ".selectIndels"
  }

  case class filterSNPs(inVCF: File, outVCF: File) extends VariantFiltration with CommandLineGATKArgs {
    this.variant = inVCF
    this.out = outVCF
    this.filterName ++= List("QDFilter", "HRunFilter", "FSFilter")
    this.filterExpression ++= List("\"QD<5.0\"", "\"HRun>5\"", "\"FS>200.0\"")

    this.scatterCount = 10
    this.analysisName = queueLogDir + outVCF + ".filterSNPs"
    this.jobName =  queueLogDir + outVCF + ".filterSNPs"
  }

  case class filterIndels(inVCF: File, outVCF: File) extends VariantFiltration with CommandLineGATKArgs {
    this.variant = inVCF
    this.out = outVCF
    this.filterName ++= List("QDFilter", "ReadPosRankSumFilter", "FSFilter")
    this.filterExpression ++= List("\"QD<2.0\"", "\"ReadPosRankSum<-20.0\"", "\"FS>200.0\"")

    this.scatterCount = 10
    this.analysisName = queueLogDir + outVCF + ".filterIndels"
    this.jobName =  queueLogDir + outVCF + ".filterIndels"
  }

  case class annotateVariants(inBam: List[java.io.File], inVCF: File, outVCF: File) extends VariantAnnotator with CommandLineGATKArgs {
    this.input_file = inBam
    this.variant = inVCF
    this.out = outVCF
    this.A ++= List("FisherStrand")

    this.scatterCount = numJobs
    this.analysisName = queueLogDir + outVCF + ".annotateVariants"
    this.jobName = queueLogDir + outVCF + ".annotateVariants"
  }

  case class recalibrateSNPs(inVCF: File, outRscript: File, outTranches: File, outRecal: File) extends VariantRecalibrator with CommandLineGATKArgs {
    //this.rodBind :+= RodBind("input", "VCF", inVCF)
    this.input :+= inVCF
    //this.rodBind :+= RodBind("hapmap", "VCF", hapmap, "known=false,training=true,truth=true,prior=15.0")
    this.resource :+= TaggedFile(hapmap, "known=false,training=true,truth=true,prior=15.0")
    //this.rodBind :+= RodBind("omni", "VCF", omni, "known=false,training=true,truth=true,prior=12.0")
    this.resource :+= TaggedFile(omni, "known=false,training=true,truth=true,prior=12.0")
    //this.rodBind :+= RodBind("dbsnp", "VCF", dbsnp, "known=true,training=false,truth=false,prior=10.0")
    this.resource :+= TaggedFile(dbsnp, "known=true,training=false,truth=false,prior=10.0")

    //error and complains about InbreedingCoeff when running on four samples
    //this.use_annotation ++= List("QD", "HaplotypeScore", "MQRankSum", "ReadPosRankSum", "FS", "MQ", "DP", "InbreedingCoeff")
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
    //this.rodBind :+= RodBind("input", "VCF", inVCF)
    this.input :+= inVCF
    //this.rodBind :+= RodBind("training", "VCF", mdindels, "known=true,training=true,truth=true,prior=12.0")
    this.resource :+= TaggedFile(mdindels, "known=true,training=true,truth=true,prior=12.0")

    //complains about InbreedingCoeff when running on 4 samples
    //this.use_annotation ++= List("QD", "FS", "HaplotypeScore", "ReadPosRankSum", "InbreedingCoeff")
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
    //this.rodBind :+= RodBind("input", "VCF", inVCF)
    this.input :+= inVCF
    this.tranches_file = inTranches
    this.recal_file = inRecal
    this.ts_filter_level = 99.0
    this.mode = VariantRecalibratorArgumentCollection.Mode.SNP
    this.out = outVCF

    this.memoryLimit = 32;
    this.analysisName = queueLogDir + outVCF + ".applyRecalibrationToSNPs"
    this.jobName =  queueLogDir + outVCF + ".applyRecalibrationToSNPs"
  }

  case class applyRecalibrationToIndels(inVCF: File, inTranches: File, inRecal: File, outVCF: File) extends ApplyRecalibration with CommandLineGATKArgs {
    //this.rodBind :+= RodBind("input", "VCF", inVCF)
    this.input :+= inVCF
    this.tranches_file = inTranches
    this.recal_file = inRecal
    this.ts_filter_level = 99.0
    this.mode = VariantRecalibratorArgumentCollection.Mode.INDEL
    this.out = outVCF

    this.memoryLimit = 32;
    this.analysisName = queueLogDir + outVCF + ".applyRecalibrationToIndels"
    this.jobName =  queueLogDir + outVCF + ".applyRecalibrationToIndels"
  }

  case class evaluateSNPs(inVCF: File, outEval: File, dbSNP: File) extends VariantEval with CommandLineGATKArgs {
    //this.rodBind :+= RodBind("eval", "VCF", inVCF)
    this.eval :+= inVCF
    //this.rodBind :+= RodBind("dbsnp", "VCF", dbsnp)
    this.D = dbSNP
    //this.VT = List(VariantContext.Type.SNP)
    this.out = outEval

    this.analysisName = queueLogDir + outEval + ".variantEvalSNPs"
    this.jobName =  queueLogDir + outEval + ".variantEvalSNPs"
  }

  case class evaluateIndels(inVCF: File, outEval: File, dbSNP: File) extends VariantEval with CommandLineGATKArgs {
    //this.rodBind :+= RodBind("eval", "VCF", inVCF)
    this.eval :+= inVCF
    //this.rodBind :+= RodBind("dbsnp", "VCF", dbsnp)
    this.D = dbSNP
    //this.VT = List(VariantContext.Type.INDEL)
    this.out = outEval

    this.analysisName = queueLogDir + outEval + ".variantEvalIndels"
    this.jobName =  queueLogDir + outEval + ".variantEvalIndels"
  }

  /****************************************************************************
  * Main script
  ****************************************************************************/

  def script = {
    val tb = QScriptUtils.createListFromFile(tumorBams)
    val nb = QScriptUtils.createListFromFile(normalBams)

    var bams: List[java.io.File] = List()
    bams ++= tb
    bams ++= nb

    val rawVariants                = "multiple_sample/.intermediate/variants/raw.vcf"

    val rawSNPs                    = "multiple_sample/.intermediate/snps/raw.vcf"
    val filteredSNPs               = "multiple_sample/hard_filtered/snps/snps.hard_filtered.vcf"
    val filteredTumorSNPs          = "multiple_sample/hard_filtered/snps/snps.hard_filtered.tumors.vcf"
    val filteredTumorSNPsEval      = "multiple_sample/hard_filtered/snps/snps.hard_filtered.tumors.eval"
    val filteredNormalSNPs         = "multiple_sample/hard_filtered/snps/snps.hard_filtered.normals.vcf"
    val filteredNormalSNPsEval     = "multiple_sample/hard_filtered/snps/snps.hard_filtered.normals.eval"

    val rawIndels                  = "multiple_sample/.intermediate/indels/raw.vcf"
    val filteredIndels             = "multiple_sample/hard_filtered/indels/indels.hard_filtered.vcf"
    val filteredTumorIndels        = "multiple_sample/hard_filtered/indels/indels.hard_filtered.tumors.vcf"
    val filteredTumorIndelsEval    = "multiple_sample/hard_filtered/indels/indels.hard_filtered.tumors.eval"
    val filteredNormalIndels       = "multiple_sample/hard_filtered/indels/indels.hard_filtered.normals.vcf"
    val filteredNormalIndelsEval   = "multiple_sample/hard_filtered/indels/indels.hard_filtered.normals.eval"

    // Tumor-specific files
    val tumorRawVariants           = "multiple_sample/.intermediate/variants/tumor/raw.vcf"
    val tumorRawAnnotatedVariants  = "multiple_sample/.intermediate/variants/tumor/raw.annotated.vcf"

    val tumorRscriptSNPs           = "multiple_sample/.intermediate/variants/tumor/snps.vqsr.R"
    val tumorTranchesSNPs          = "multiple_sample/.intermediate/variants/tumor/snps.tranches"
    val tumorRecalSNPs             = "multiple_sample/.intermediate/variants/tumor/snps.recal"

    val tumorRscriptIndels         = "multiple_sample/.intermediate/variants/tumor/indels.vqsr.R"
    val tumorTranchesIndels        = "multiple_sample/.intermediate/variants/tumor/indels.tranches"
    val tumorRecalIndels           = "multiple_sample/.intermediate/variants/tumor/indels.recal"

    val tumorRecalibratedSNPs      = "multiple_sample/.intermediate/variants/tumor/partially_recalibrated.vcf"

    val tumorRecalibratedVariants  = "multiple_sample/soft_filtered/tumor/tumor.soft_filtered.vcf"

    //JW added the following two lines
    val tumorFinalSNPs             = "multiple_sample/soft_filtered/tumor/tumorSNPs.soft_filtered.vcf"
    val tumorFinalIndels           = "multiple_sample/soft_filtered/tumor/tumorIndels.soft_filtered.vcf"

    val tumorEvalSNPs              = "multiple_sample/soft_filtered/tumor/tumor.soft_filtered.snps.eval"
    val tumorEvalIndels            = "multiple_sample/soft_filtered/tumor/tumor.soft_filtered.indels.eval"

    // Normal-specific files
    val normalRawVariants          = "multiple_sample/.intermediate/variants/normal/raw.vcf"
    val normalRawAnnotatedVariants = "multiple_sample/.intermediate/variants/normal/raw.annotated.vcf"

    val normalRscriptSNPs          = "multiple_sample/.intermediate/variants/normal/snps.vqsr.R"
    val normalTranchesSNPs         = "multiple_sample/.intermediate/variants/normal/snps.tranches"
    val normalRecalSNPs            = "multiple_sample/.intermediate/variants/normal/snps.recal"

    val normalRscriptIndels        = "multiple_sample/.intermediate/variants/normal/indels.vqsr.R"
    val normalTranchesIndels       = "multiple_sample/.intermediate/variants/normal/indels.tranches"
    val normalRecalIndels          = "multiple_sample/.intermediate/variants/normal/indels.recal"

    val normalRecalibratedSNPs     = "multiple_sample/.intermediate/variants/normal/partially_recalibrated.vcf"

    val normalRecalibratedVariants = "multiple_sample/soft_filtered/normal/normal.soft_filtered.vcf"

    //JW added the following two lines
    val normalFinalSNPs             = "multiple_sample/soft_filtered/normal/normalSNPs.soft_filtered.vcf"
    val normalFinalIndels           = "multiple_sample/soft_filtered/normal/normalIndels.soft_filtered.vcf"

    val normalEvalSNPs             = "multiple_sample/soft_filtered/normal/normal.soft_filtered.snps.eval"
    val normalEvalIndels           = "multiple_sample/soft_filtered/normal/normal.soft_filtered.indels.eval"


    add(
      callVariants(bams, rawVariants, dbsnp),

      // HARD FILTERS:
      // filter
      selectSNPs(rawVariants, rawSNPs),
      filterSNPs(rawSNPs, filteredSNPs),

      selectIndels(rawVariants, rawIndels),
      filterIndels(rawIndels, filteredIndels),

      // evaluate snps
      selectSamples(filteredSNPs, tumorSamples, filteredTumorSNPs),
      evaluateSNPs(filteredTumorSNPs, filteredTumorSNPsEval, dbsnp),

      selectSamples(filteredSNPs, normalSamples, filteredNormalSNPs),
      evaluateSNPs(filteredNormalSNPs, filteredNormalSNPsEval, dbsnp),

      // evaluate indels
      selectSamples(filteredIndels, tumorSamples, filteredTumorIndels),
      evaluateIndels(filteredTumorIndels, filteredTumorIndelsEval, dbsnp),

      selectSamples(filteredIndels, normalSamples, filteredNormalIndels),
      evaluateIndels(filteredNormalIndels, filteredNormalIndelsEval, dbsnp),

      // SOFT FILTERS:
      // tumor
      selectSamples(rawVariants, tumorSamples, tumorRawVariants),
      annotateVariants(tb, tumorRawVariants, tumorRawAnnotatedVariants),
      recalibrateSNPs(tumorRawAnnotatedVariants, tumorRscriptSNPs, tumorTranchesSNPs, tumorRecalSNPs),
      recalibrateIndels(tumorRawAnnotatedVariants, tumorRscriptIndels, tumorTranchesIndels, tumorRecalIndels),
      applyRecalibrationToSNPs(tumorRawAnnotatedVariants, tumorTranchesSNPs, tumorRecalSNPs, tumorRecalibratedSNPs),
      applyRecalibrationToIndels(tumorRecalibratedSNPs, tumorTranchesIndels, tumorRecalIndels, tumorRecalibratedVariants),

      // evaluateSNPs(tumorRecalibratedVariants, tumorEvalSNPs, dbsnp),
      // evaluateIndels(tumorRecalibratedVariants, tumorEvalIndels, dbsnp),

      // JW added the following 4 lines
      selectSNPs(tumorRecalibratedVariants, tumorFinalSNPs),
      selectIndels(tumorRecalibratedVariants, tumorFinalIndels),
      evaluateSNPs(tumorFinalSNPs, tumorEvalSNPs, dbsnp),
      evaluateIndels(tumorFinalIndels, tumorEvalIndels, dbsnp),

      // normal
      selectSamples(rawVariants, normalSamples, normalRawVariants),
      annotateVariants(nb, normalRawVariants, normalRawAnnotatedVariants),
      recalibrateSNPs(normalRawAnnotatedVariants, normalRscriptSNPs, normalTranchesSNPs, normalRecalSNPs),
      recalibrateIndels(normalRawAnnotatedVariants, normalRscriptIndels, normalTranchesIndels, normalRecalIndels),
      applyRecalibrationToSNPs(normalRawAnnotatedVariants, normalTranchesSNPs, normalRecalSNPs, normalRecalibratedSNPs),
      applyRecalibrationToIndels(normalRecalibratedSNPs, normalTranchesIndels, normalRecalIndels, normalRecalibratedVariants),
      // evaluateSNPs(normalRecalibratedVariants, normalEvalSNPs, dbsnp),
      // evaluateIndels(normalRecalibratedVariants, normalEvalIndels, dbsnp)

      // JW added the following 4 lines
      selectSNPs(normalRecalibratedVariants, normalFinalSNPs),
      selectIndels(normalRecalibratedVariants, normalFinalIndels),
      evaluateSNPs(normalFinalSNPs, normalEvalSNPs, dbsnp),
      evaluateIndels(normalFinalIndels, normalEvalIndels, dbsnp)
    )
  }
}
