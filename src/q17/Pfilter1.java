package q17;

import cores.avro.FilterOperator;

public class Pfilter1 implements FilterOperator<String> {
    String br1;
    String br2;

    public Pfilter1(String s1, String s2) {
        br1 = s1;
        br2 = s2;
    }

    public String getName() {
        return "p_brand";
    }

    public boolean isMatch(String s) {
        if (s.compareTo(br1) >= 0 && s.compareTo(br2) <= 0) {
            return true;
        } else
            return false;
    }
}
