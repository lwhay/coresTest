package function;

public interface IBinaryRelation<T, L, R> {
    boolean nonBetween(T t, L l, R r);

    boolean fullBetween(T t, L l, R r);

    boolean leftBetween(T t, L l, R r);

    boolean rightBetween(T t, L l, R r);
}
