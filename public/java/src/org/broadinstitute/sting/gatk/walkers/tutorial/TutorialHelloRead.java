package org.broadinstitute.sting.gatk.walkers.tutorial;

import net.sf.samtools.SAMRecord;
import org.broadinstitute.sting.commandline.Output;
import org.broadinstitute.sting.gatk.contexts.ReferenceContext;
import org.broadinstitute.sting.gatk.refdata.ReadMetaDataTracker;
import org.broadinstitute.sting.gatk.walkers.ReadWalker;

import java.io.PrintStream;

/**
 * A test program that prints out read names and their genomic position.
 *
 * This is a tutorial program for learning how to write simple programs in the GATK.
 */
public class TutorialHelloRead extends ReadWalker<Integer, Integer> {
    // First, we'll declare an output stream.  We don't need to instantiate the stream -
    // the GATK will handle hooking up the output properly for us.
    @Output
    public PrintStream out;

    @Override
    public Integer map(ReferenceContext ref, SAMRecord read, ReadMetaDataTracker metaDataTracker) {
        // The map() function gets invoked for each read in the file.  It's easy to
        // print out information about that read to the terminal, like so:
        out.println("Hello, " + read.getReadName() +
                       " at " + read.getReferenceName() +
                          ":" + read.getAlignmentStart()
        );
        return null;
    }

    @Override
    public Integer reduceInit() {
        return null;
    }

    @Override
    public Integer reduce(Integer value, Integer sum) {
        return null;
    }
}
