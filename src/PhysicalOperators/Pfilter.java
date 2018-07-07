package PhysicalOperators;

import cores.avro.FilterOperator;

public class Pfilter implements FilterOperator<String> {
    String color;

    public Pfilter(String s) {
        color = s;
    }

    public String getName() {
        return "p_name";
    }

    public boolean isMatch(String s) {
        if (s.contains(color))
            return true;
        else
            return false;
    }
}
