package org.broadinstitute.sting.queue.extensions.picard

import org.broadinstitute.sting.commandline._

import java.io.File

class CleanSam extends org.broadinstitute.sting.queue.function.JavaCommandLineFunction with PicardBamFunction {
  this.sortOrder = null

  analysisName = "CleanSam"
  javaMainClass = "net.sf.picard.sam.CleanSam"

  @Input(doc="The input SAM or BAM files to clean.", shortName = "input", fullName = "input_bam_files", required = true)
  var input: List[File] = _

  @Output(doc="The cleaned BAM or SAM output file.", shortName = "output", fullName = "output_bam_file", required = true)
  var output: File = _

  override def inputBams = input
  override def outputBam = output
  override def commandLine = super.commandLine
}
