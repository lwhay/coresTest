/**
 * 
 */
package function;

/**
 * @author Michael
 *
 */
public class IntRelation implements IUnaryRelaion<Integer, Integer> {

    @Override
    public boolean lt(Integer l, Integer r) {
        // TODO Auto-generated method stub
        return l.compareTo(r) < 0;
    }

    @Override
    public boolean gt(Integer l, Integer r) {
        // TODO Auto-generated method stub
        return l.compareTo(r) > 0;
    }

    @Override
    public boolean le(Integer l, Integer r) {
        // TODO Auto-generated method stub
        return l.compareTo(r) <= 0;
    }

    @Override
    public boolean ge(Integer l, Integer r) {
        // TODO Auto-generated method stub
        return l.compareTo(r) >= 0;
    }

    @Override
    public boolean eq(Integer l, Integer r) {
        // TODO Auto-generated method stub
        return l.equals(r);
    }

    @Override
    public boolean ne(Integer l, Integer r) {
        // TODO Auto-generated method stub
        return !l.equals(r);
    }

}
