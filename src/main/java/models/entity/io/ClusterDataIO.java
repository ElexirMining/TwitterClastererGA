package models.entity.io;

import java.io.Serializable;
import java.util.List;

/**
 * Модель данных кластера, с указанием типа центра кластера и входящих элементов
 * @param <C> Тип центра кластера
 * @param <T> Тип входящих элементов
 */
public class ClusterDataIO<C, T> implements Serializable {
    public Integer Number;
    public C Center;
    public List<T> Items;

    public ClusterDataIO(Integer number, C center, List<T> items)
    {
        Number = number;
        Center = center;
        Items = items;
    }
}
