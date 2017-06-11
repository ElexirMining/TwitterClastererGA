package models.entity;

import java.io.Serializable;

/**
 * Модель данных кластера, с указанием типа центра кластера и входящих элементов
 * @param <C> Тип центра кластера
 * @param <T> Тип входящих элементов
 */
public class ClusterData<C, T> implements Serializable {
    public Integer Number;
    public C Center;
    public Iterable<T> Items;

    public ClusterData(Integer number,C center, Iterable<T> items)
    {
        Number = number;
        Center = center;
        Items = items;
    }
}
