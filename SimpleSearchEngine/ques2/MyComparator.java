package retrieve;

import java.util.Comparator;
import java.util.Map;

class MyComparator implements Comparator {
    Map map1;
    public MyComparator(Map map1) {
        this.map1 = map1;
    }
    public int compare(Object a, Object b) {
        if ((Integer)map1.get(a)>=(Integer)map1.get(b)) {
            return -1; // For ascending, return -1;
        } else {
            return 1; // For ascending, return 1;
        } // returning 0 would merge keys
    }
}