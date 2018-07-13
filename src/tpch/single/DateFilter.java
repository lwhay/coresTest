/**
 * 
 */
package tpch.single;

import cores.avro.FilterOperator;

/**
 * @author Michael
 *
 */
public class DateFilter implements FilterOperator<String> {
    String ts;

    public DateFilter(String ts) {
        this.ts = ts;
    }

    @Override
    public String getName() {
        return "l_shipdate";
    }

    @Override
    public boolean isMatch(String t) {
        return t.compareTo(ts) <= 0;
    }

}
