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

class TestVariantEval extends QScript {
  qscript =>

  @Input(doc="Reference fasta file", fullName="reference", shortName="R", required=true)
  var reference: File = _

  @Input(doc="DbSNP file", fullName="dbsnp", shortName="D", required=true)
  var dbsnp: File = _

  @Input(doc="VCF file", fullName="variant", shortName="V", required=true)
  var variant: File = _

  trait CommandLineGATKArgs extends CommandLineGATK {
    this.memoryLimit = 4;
    this.reference_sequence = qscript.reference
  }

  case class evaluateSNPs(inVCF: File, outEval: File, dbSNP: File) extends VariantEval with CommandLineGATKArgs {
    this.eval :+= inVCF
    this.D = dbSNP
    this.out = outEval

    this.analysisName = "tests/test.eval.analysis"
    this.jobName =  "tests/test.eval.log"
  }

  /****************************************************************************
  * Main script
  ****************************************************************************/

  def script = {
    val eval = "tests/test.eval"

    add( evaluateSNPs(variant, eval, dbsnp) )
  }
}
