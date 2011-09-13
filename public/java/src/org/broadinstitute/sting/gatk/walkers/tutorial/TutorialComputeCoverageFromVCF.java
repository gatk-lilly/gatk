package org.broadinstitute.sting.gatk.walkers.tutorial;

import org.broadinstitute.sting.commandline.Argument;
import org.broadinstitute.sting.commandline.Output;
import org.broadinstitute.sting.gatk.contexts.AlignmentContext;
import org.broadinstitute.sting.gatk.contexts.ReferenceContext;
import org.broadinstitute.sting.gatk.refdata.RefMetaDataTracker;
import org.broadinstitute.sting.gatk.walkers.RodWalker;
import org.broadinstitute.sting.utils.variantcontext.VariantContext;

import java.io.PrintStream;
import java.util.Collection;
import java.util.TreeMap;

/**
 * A test program that prints out a depth of coverage histogram using the VCF annotations.  While this is not nearly as
 * capable as the DepthOfCoverage tool, this might be easier for a quick look at the coverage for a sample.
 *
 * This is a tutorial program for learning how to write simple programs in the GATK.
 */
public class TutorialComputeCoverageFromVCF extends RodWalker<Integer, Integer> {
    @Output
    public PrintStream out;

    @Argument(fullName="sample", shortName="sn", doc="Sample to process (leave unspecified to process all samples", required=false)
    public String SAMPLE;

    // A TreeMap is a hashtable object that returns its keys in sorted order.  Very convenient for things like histograms.
    private TreeMap<Integer, Integer> histogram = new TreeMap<Integer, Integer>();

    @Override
    public Integer map(RefMetaDataTracker tracker, ReferenceContext ref, AlignmentContext context) {
        if (tracker != null) {
            Collection<VariantContext> vcs = tracker.getVariantContexts(ref, "variant", null, context.getLocation(), true, true);

            for (VariantContext vc : vcs) {
                // Let's get the depth of coverage for the site from the annotations in the VCF file.  If we're not provided
                // a sample, we'll get it from the "DP" annotation in the INFO field.  Otherwise, we'll get it from the "DP"
                // annotation in the appropriate genotype column.
                int depth = 0;
                if (SAMPLE == null) depth = vc.getAttributeAsIntegerNoException("DP");
                else if (vc.getGenotype(SAMPLE).isCalled()) depth = vc.getGenotype(SAMPLE).getAttributeAsIntegerNoException("DP");

                int count = histogram.containsKey(depth) ? histogram.get(depth) : 0;

                // Increment the counter in the histogram.
                histogram.put(depth, count+1);
            }
        }
        return null;
    }

    @Override
    public Integer reduceInit() { return null; }

    @Override
    public Integer reduce(Integer value, Integer sum) { return null; }

    public void onTraversalDone(Integer sum) {
        // Output the histogram (first column: depth, second column: number of records seen at given depth).
        for (int depth : histogram.keySet()) {
            out.printf("%d %d%n", depth, histogram.get(depth));
        }
    }
}