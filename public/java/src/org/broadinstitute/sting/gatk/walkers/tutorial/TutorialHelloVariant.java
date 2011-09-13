package org.broadinstitute.sting.gatk.walkers.tutorial;

import org.broadinstitute.sting.commandline.Output;
import org.broadinstitute.sting.gatk.contexts.AlignmentContext;
import org.broadinstitute.sting.gatk.contexts.ReferenceContext;
import org.broadinstitute.sting.gatk.refdata.RefMetaDataTracker;
import org.broadinstitute.sting.gatk.walkers.RodWalker;
import org.broadinstitute.sting.utils.variantcontext.VariantContext;

import java.io.PrintStream;
import java.util.Collection;

/**
 * A test program that prints out variant information and their genomic position.
 *
 * This is a tutorial program for learning how to write simple programs in the GATK.
 */
public class TutorialHelloVariant extends RodWalker<Integer, Integer> {
    @Output
    public PrintStream out;

    @Override
    public Integer map(RefMetaDataTracker tracker, ReferenceContext ref, AlignmentContext context) {
        // First, verify that the metadata tracker is not null (meaning there is a variant at this locus to process).
        if (tracker != null) {
            // Get all of the "VariantContext" objects that span this locus.  A VariantContext represents a line in a VCF file.
            Collection<VariantContext> vcs = tracker.getVariantContexts(ref, "variant", null, context.getLocation(), true, false);

            // There may be more than one variant at this locus.  Process them all.
            for (VariantContext vc : vcs) {
                out.println("Hello, ref=" + vc.getReference() +
                                  ",alt=" + vc.getAltAlleleWithHighestAlleleCount() +
                                   " at " + vc.getChr() +
                                      ":" + vc.getStart()
                );
            }

            // Return 1, indicating that we saw a variant.
            return 1;
        }

        // We saw nothing of interest, so return 0.
        return 0;
    }

    @Override
    public Integer reduceInit() {
        // The reduce() function aggregates results from the map() function.  In this program we're counting the number
        // of records we saw by returning 1 from map() and adding up the results in reduce(), but we still need to
        // initialize reduce's counter to something (so that the very first call to reduce() has some initial entry
        // for the 'sum' argument.  We'll set it to zero here.
        return 0;
    }

    @Override
    public Integer reduce(Integer value, Integer sum) {
        // All of the return values from map() will be provided to be reduce so that results from independent map() calls
        // can be combined to compute some final result on all of the data.  Here, we'll compute the number of records
        // that we processed in total.
        return value + sum;
    }

    public void onTraversalDone(Integer sum) {
        // This method gets called at the very end of the computation, when all of the map() and reduce() calls are done.
        // Report the number of records that we processed in total.  This is the result of all of the reduce() operations
        // on the map() return value.
        out.println("Said hello to " + sum + " records.");
    }
}
