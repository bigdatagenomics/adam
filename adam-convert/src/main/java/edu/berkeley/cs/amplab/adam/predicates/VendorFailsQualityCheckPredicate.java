package edu.berkeley.cs.amplab.adam.predicates;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord;

public class VendorFailsQualityCheckPredicate implements Predicate<ADAMRecord> {

    @Override
    public boolean apply(ADAMRecord input) {
        Preconditions.checkNotNull(input);
        return Optional.fromNullable(input.getReadFailedVendorQualityCheckFlag()).or(Boolean.FALSE);
    }
}
