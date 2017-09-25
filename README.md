# Flat file data munger

Sometimes `awk`, `grep`, and `sed` aren't available. For those times, `flt`
is a simple tool for manipulating and reshaping flat files commonly used by
data analysts and data scientists.

## Examples

*Take a specific set of rows*

    flt input.csv --skip 3 --nrows 100

*Take a specific set of columns*

    flt input.csv --columns "ID,GEONAME,POPULATION"

*Filter by a predicate*

    flt input.csv --predicate "GEONAME='British Columbia'"

*Convert formats*

    flt input.csv --format json

    flt input.csv --format libsvm --libsvm-label "AMBULANCE_CALLS"
