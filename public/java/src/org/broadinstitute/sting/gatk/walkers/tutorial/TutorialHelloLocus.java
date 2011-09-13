package org.broadinstitute.sting.gatk.walkers.tutorial;

import org.broadinstitute.sting.commandline.Output;
import org.broadinstitute.sting.gatk.contexts.AlignmentContext;
import org.broadinstitute.sting.gatk.contexts.ReferenceContext;
import org.broadinstitute.sting.gatk.refdata.RefMetaDataTracker;
import org.broadinstitute.sting.gatk.walkers.LocusWalker;

import java.io.PrintStream;

/**
 * A test program that prints out the depth of coverage at a locus given a BAM file.
 *
 * This is a tutorial program for learning how to write simple programs in the GATK.
 */
public class TutorialHelloLocus extends LocusWalker<Integer, Integer> {
    @Output
    public PrintStream out;

    @Override
    public Integer map(RefMetaDataTracker tracker, ReferenceContext ref, AlignmentContext context) {
        // The AlignmentContext objects holds information about the bases seen at a locus.  This includes strand
        // information, mapping quality, base counts for each base, etc., and works properly in the presence of
        // insertions and deletions.  No need to grab reads and line them up manually to get a proper pileup.

        out.printf("Hello, %d at %s%n", context.getBasePileup().size(), ref.getLocus());

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
