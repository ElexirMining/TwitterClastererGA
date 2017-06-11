package io;

import java.io.File;

/**
 * Класс, отвечающий за работу с файлами
 */
public abstract class FileManager {
    protected String _path;
    protected File _file;

    public FileManager(String path) {
        this._path = path;
        this._file = new File(_path);
    }

    /**
     * Метод, отвечающий за проверку наличия пути к файлу
     * @return
     */
    public abstract boolean IsPathExists();

    /**
     * Метод, отвечающий за создание пути к файлу
     */
    public abstract void CreatePath();

    /**
     * Метод, отвечающий за создание пустого файла
     */
    public abstract void CreateFile();

    /**
     * Метод, отвечающий за запись данных в файл
     */
    public abstract void Write(String text);

    /**
     * Метод, отвечающий за запись данных в файл
     */
    public abstract void Write(byte[] bytes);

    /**
     * Метод, отвечающий за чтение данных из файла
     * @return строка с данными
     */
    public abstract String ReadString();

    /**
     * Метод, отвечающий за чтение данных из файла
     * @return набор байтов
     */
    public abstract byte[] ReadBytes();

    /**
     * Метод, отвечающий за проверку перед записью файла
     */
    void CheckFile(){
        if (!IsPathExists()) CreateFile();
    }
}
