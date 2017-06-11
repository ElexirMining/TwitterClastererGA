package io;

public interface IDataProvider<T> {
    /**
     * Метод, отвечающий за построение транзакции
     * @param sourceData данные для построения транзакции
     */
    void Build(T sourceData);
    /**
     * Метод, отвечающий за сохранения данных
     */
    void Save();
}
