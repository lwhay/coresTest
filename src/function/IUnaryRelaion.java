/**
 * 
 */
package function;

/**
 * @author Michael
 *
 */
public interface IUnaryRelaion<L, R> {
    boolean lt(L l, R r);

    boolean gt(L l, R r);

    boolean le(L l, R r);

    boolean ge(L l, R r);

    boolean eq(L l, R r);

    boolean ne(L l, R r);
}
