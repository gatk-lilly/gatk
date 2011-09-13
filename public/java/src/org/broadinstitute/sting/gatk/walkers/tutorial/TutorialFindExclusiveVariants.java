package org.broadinstitute.sting.gatk.walkers.tutorial;

import org.broadinstitute.sting.commandline.Argument;
import org.broadinstitute.sting.commandline.Output;
import org.broadinstitute.sting.gatk.contexts.AlignmentContext;
import org.broadinstitute.sting.gatk.contexts.ReferenceContext;
import org.broadinstitute.sting.gatk.refdata.RefMetaDataTracker;
import org.broadinstitute.sting.gatk.walkers.RodWalker;
import org.broadinstitute.sting.utils.SampleUtils;
import org.broadinstitute.sting.utils.codecs.vcf.VCFHeader;
import org.broadinstitute.sting.utils.codecs.vcf.VCFUtils;
import org.broadinstitute.sting.utils.codecs.vcf.VCFWriter;
import org.broadinstitute.sting.utils.variantcontext.VariantContext;

import java.util.Collection;

/**
 * A test program that finds a variant exclusive to a single sample (as specified by the user) and prints out a new
 * VCF with just those lines.  Something like this is handy for finding things in a tumor sample but absent in a
 * normal sample.
 *
 * This is a tutorial program for learning how to write simple programs in the GATK.
 */
public class TutorialFindExclusiveVariants extends RodWalker<Integer, Integer> {
    @Output(doc="File to which variants should be written", required=true)
    public VCFWriter vcf;

    @Argument(fullName="sample", shortName="sn", doc="Sample to whom the variant should be exclusive", required=true)
    public String SAMPLE;

    // The initialize() method gets called *before* any calls to map() or reduce(), so it can be used to prepare
    // the environment for the computation, open files, write headers, etc.
    public void initialize() {
        // Start a VCF file by outputting a header.  We can add more header lines if we choose, or add/remove samples,
        // etc., but here we'll just stick with the headers of the input VCF file.
        vcf.writeHeader( new VCFHeader( VCFUtils.getHeaderFields(this.getToolkit()),
                                        SampleUtils.getUniqueSamplesFromRods(this.getToolkit()) ) );
    }

    @Override
    public Integer map(RefMetaDataTracker tracker, ReferenceContext ref, AlignmentContext context) {
        if (tracker != null) {
            Collection<VariantContext> vcs = tracker.getVariantContexts(ref, "variant", null, context.getLocation(), true, true);

            for (VariantContext vc : vcs) {
                // A VariantContext contains a collection of Genotype objects, each of which represents the sample-specific
                // data at this locus.  Genotypes can be interrogated for their name, genotype status, and other metadata
                // like depth, genotype likelihoods, etc.
                boolean sampleIsHet = vc.getGenotype(SAMPLE).isHet();
                boolean othersAreNonVariant = true;

                for (String otherSample : vc.getSampleNames()) {
                    if (!SAMPLE.equals(otherSample) && (vc.getGenotype(otherSample).isHet() || vc.getGenotype(otherSample).isHomVar())) {
                        othersAreNonVariant = false;
                    }
                }

                if (sampleIsHet && othersAreNonVariant) {
                    // We've found a record exclusive to our sample of interest.  Let's add it to the VCF file.  We
                    // must also specify the reference base (so that the VCF writer knows how to tell the difference
                    // between homozygous-reference and homozygous-variant.

                    vcf.add(vc, ref.getBase());
                }
            }
        }

        return null;
    }

    @Override
    public Integer reduceInit() { return null; }

    @Override
    public Integer reduce(Integer value, Integer sum) { return null; }
}
