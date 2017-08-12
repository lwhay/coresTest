package q17;

import cores.avro.FilterOperator;

public class Pfilter2 implements FilterOperator<String> {
    String[] start;

    public Pfilter2(String[] s) {
        start = s;
    }

    public String getName() {
        return "p_container";
    }

    public boolean isMatch(String s) {
        for (int i = 0; i < start.length; i++) {
            if (s.startsWith(start[i]))
                return true;
        }
        return false;
        //        if (s.equals("MED BOX")) {
        //            return true;
        //        } else
        //            return false;
    }
}
