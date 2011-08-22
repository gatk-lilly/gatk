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

  @Input(doc="The number of scatter-gather jobs to use", fullName="numJobs", shortName="j", required=false)
  var numJobs: Int = 1

  val queueLogDir: String = ".qlog/"  // Gracefully hide Queue's output

  trait CommandLineGATKArgs extends CommandLineGATK {
    this.memoryLimit = 4;
    this.reference_sequence = qscript.reference
    this.isIntermediate = false
  }

  case class callSNPs(inBam: List[java.io.File], outVCF: File) extends UnifiedGenotyper with CommandLineGATKArgs {
    this.input_file = inBam
    this.rodBind :+= RodBind("dbsnp", "VCF", dbsnp)
    this.out = outVCF
    this.glm = org.broadinstitute.sting.gatk.walkers.genotyper.GenotypeLikelihoodsCalculationModel.Model.SNP
    this.baq = org.broadinstitute.sting.utils.baq.BAQ.CalculationMode.CALCULATE_AS_NECESSARY
    this.A ++= List("FisherStrand", "FS", "InbreedingCoeff")

    this.scatterCount = numJobs
    this.analysisName = queueLogDir + outVCF + ".callSNPs"
    this.jobName = queueLogDir + outVCF + ".callSNPs"
  }

  case class callIndels(inBam: List[java.io.File], outVCF: File) extends UnifiedGenotyper with CommandLineGATKArgs {
    this.input_file = inBam
    this.rodBind :+= RodBind("dbsnp", "VCF", dbsnp)
    this.out = outVCF
    this.glm = org.broadinstitute.sting.gatk.walkers.genotyper.GenotypeLikelihoodsCalculationModel.Model.INDEL
    this.baq = org.broadinstitute.sting.utils.baq.BAQ.CalculationMode.OFF

    this.scatterCount = numJobs
    this.analysisName = queueLogDir + outVCF + ".callIndels"
    this.jobName = queueLogDir + outVCF + ".callIndels"
  }

  case class selectSamples(inVCF: File, inSamples: File, outVCF: File) extends SelectVariants with CommandLineGATKArgs {
    this.variantVCF = inVCF
    this.sample_file ++= List(inSamples)
    this.out = outVCF
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

  case class VQSR(inVCF: File, outRscript: File, outTranches: File, outRecal: File) extends VariantRecalibrator with CommandLineGATKArgs {
    this.rodBind :+= RodBind("input", "VCF", inVCF)
    this.rodBind :+= RodBind("hapmap", "VCF", hapmap, "known=false,training=true,truth=true,prior=15.0")
    this.rodBind :+= RodBind("omni", "VCF", omni, "known=false,training=true,truth=true,prior=12.0")
    this.rodBind :+= RodBind("dbsnp", "VCF", dbsnp, "known=true,training=false,truth=false,prior=10.0")
    this.use_annotation ++= List("QD", "HaplotypeScore", "MQRankSum", "ReadPosRankSum", "HRun", "FS")
    this.allPoly = true
    this.tranche ++= List("100.0", "99.9", "99.5", "99.3", "99.0", "98.9", "98.8", "98.5", "98.4", "98.3", "98.2", "98.1", "98.0", "97.9", "97.8", "97.5", "97.0", "95.0", "90.0")
    this.rscript_file = outRscript
    this.tranches_file = tranchesFile
    this.recal_file = recalFile

    this.analysisName = queueLogDir + outRecal + ".VQSR"
    this.jobName =  queueLogDir + outRecal + ".VQSR"
  }

  case class applyVQSR(inVCF: File, inTranches: File, inRecal: File, outVCF: File) extends ApplyRecalibration with CommandLineGATKArgs {
    this.rodBind :+= RodBind("input", "VCF", inVCF)
    this.tranches_file = inTranches
    this.recal_file = inRecal
    this.ts_filter_level = 99.0
    this.out = outVCF

    this.analysisName = queueLogDir + outVCF + ".applyVQSR"
    this.jobName =  queueLogDir + outVCF + ".applyVQSR"
  }

  case class combineTumorNormalCalls(inTumorVCF: File, inNormalVCF: File, outVCF: File) extends CombineVariants with CommandLineGATKArgs {
    this.rodBind :+= RodBind("tumor", "VCF", inTumorVCF)
    this.rodBind :+= RodBind("normal", "VCF", inNormalVCF)
    //this.priority = "tumor,normal"
    this.out = outVCF

    this.analysisName = queueLogDir + outVCF + ".combineVariants"
    this.jobName =  queueLogDir + outVCF + ".combineVariants"
  }

  /****************************************************************************
  * Main script
  ****************************************************************************/

  def script = {
    val bams = QScriptUtils.createListFromFile(input)

    // SNPs
    val rawSNPs =                "intermediate/snps/snps.raw.vcf"

    val tumorRawSNPs =           "intermediate/snps/tumor/snps.raw.vcf"
    val tumorRscript =           "intermediate/snps/tumor/snps.vqsr.R"
    val tumorTranches =          "intermediate/snps/tumor/snps.tranches"
    val tumorRecal =             "intermediate/snps/tumor/snps.recal"
    val tumorRecalibratedSNPs =  "intermediate/snps/tumor/snps.analysis_ready.vcf"
    
    val normalRawSNPs =          "intermediate/snps/normal/snps.raw.vcf"
    val normalRscript =          "intermediate/snps/normal/snps.vqsr.R"
    val normalTranches =         "intermediate/snps/normal/snps.tranches"
    val normalRecal =            "intermediate/snps/normal/snps.recal"
    val normalRecalibratedSNPs = "intermediate/snps/normal/snps.analysis_ready.vcf"

    val analysisReadySNPs =      "tumornormal.snps.analysis_ready.vcf"

    // Indels
    val rawIndels =              "intermediate/indels/indels.raw.vcf"

    val tumorRawIndels =         "intermediate/indels/tumor/indels.raw.vcf"
    val tumorFilteredIndels =    "intermediate/indels/tumor/indels.analysis_ready.vcf"

    val normalRawIndels =        "intermediate/indels/normal/indels.raw.vcf"
    val normalFilteredIndels =   "intermediate/indels/normal/indels.analysis_ready.vcf"

    val analysisReadyIndels =    "tumornormal.indels.analysis_ready.vcf"

    // Add the rules
    add(
        // SNP rules
        callSNPs(bams, rawSNPs),

        selectSamples(rawSNPs, tumorSamples, tumorRawSNPs),
        VQSR(tumorRawSNPs, tumorRscript, tumorTranches, tumorRecal),
        applyVQSR(tumorRawSNPs, tumorTranches, tumorRecal, tumorRecalibratedSNPs),
    
        selectSamples(rawSNPs, normalSamples, normalRawSNPs),
        VQSR(normalRawSNPs, normalRscript, normalTranches, normalRecal),
        applyVQSR(normalRawSNPs, normalTranches, normalRecal, normalRecalibratedSNPs),

        combineTumorNormalCalls(tumorRecalibratedSNPs, normalRecalibratedSNPs, analysisReadySNPs),

        // Indel rules
        callIndels(bams, rawIndels),

        selectSamples(rawIndels, tumorSamples, tumorRawIndels),
        filterIndels(tumorRawIndels, tumorFilteredIndels),

        selectSamples(rawIndels, normalSamples, normalRawIndels),
        filterIndels(normalRawIndels, normalFilteredIndels),

        combineTumorNormalCalls(tumorFilteredIndels, normalFilteredIndels, analysisReadyIndels)
    )
  }
}
