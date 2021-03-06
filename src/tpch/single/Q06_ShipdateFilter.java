package tpch.single;

import cores.avro.FilterOperator;

public class Q06_ShipdateFilter implements FilterOperator<String> {
    String t1, t2;

    public Q06_ShipdateFilter(String t1, String t2) {
        this.t1 = t1;
        this.t2 = t2;
    }

    public String getName() {
        return "l_shipdate";
    }

    public boolean isMatch(String s) {
        if (s.compareTo(t1) >= 0 && s.compareTo(t2) < 0)
            return true;
        else
            return false;
    }
}
