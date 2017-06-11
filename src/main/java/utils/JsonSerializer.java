package utils;

import io.FileManager;

/**
 * Утилита для сериализации в Json
 */
public class JsonSerializer {
    /**
     * Загрузить объект из файла
     * @param fileManager Менеджер файла
     * @param tClass Класс объекта
     * @param <T> Тип объекта
     * @return Объект указанного типа
     */
    public static <T> T Load(FileManager fileManager, Class<T> tClass)
    {
        String line;
        //Считываем файл, если он пуст, или не существует, то возвращаем пустое значение null
        if ((line = fileManager.ReadString()) == null) return null;
        //Если текст получен, десериализуем при помощи Gson
        return GsonSingleton.Gson.fromJson(line, tClass);
    }

    /**
     * Сохранить объект в файл
     * @param fileManager Менеджер файла
     * @param object Объект для сериализации
     */
    public static void Dump(FileManager fileManager, Object object){
        //Сериализуем при помощи Gson и записываем текст в файл
        fileManager.Write(GsonSingleton.Gson.toJson(object));
    }
}
