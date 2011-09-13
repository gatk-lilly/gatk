package org.broadinstitute.sting.gatk.walkers.tutorial;

import net.sf.samtools.SAMFileWriter;
import net.sf.samtools.SAMRecord;
import org.broadinstitute.sting.commandline.Output;
import org.broadinstitute.sting.gatk.contexts.ReferenceContext;
import org.broadinstitute.sting.gatk.refdata.ReadMetaDataTracker;
import org.broadinstitute.sting.gatk.walkers.ReadWalker;

/**
 * A test program that prepends the read group's platform unit tag to the read name and
 * generates a new BAM file with the modified data. This is useful when the input data
 * has non-globally-unique read names and needs to be sanitized before downstream analysis.
 *
 * This is a tutorial program for learning how to write simple programs in the GATK.
 */
public class TutorialFixReadNames extends ReadWalker<Integer, Integer> {
    // First, we'll declare a *BAM* output writer (and let the GATK take care of the details
    // of instantiating it, setting the BAM header, hooking it up to the -o argument, etc.
    @Output
    SAMFileWriter out;

    @Override
    public Integer map(ReferenceContext ref, SAMRecord read, ReadMetaDataTracker metaDataTracker) {
        // Let's modify the read name, prepending the platform unit tag.
        read.setReadName(read.getReadGroup().getPlatformUnit() + "." + read.getReadName());

        // Now, we'll add the read to the output stream.
        out.addAlignment(read);

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
