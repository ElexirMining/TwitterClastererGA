package io;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

public class WindowsFileManager extends FileManager {

    public WindowsFileManager(String path) {
        super(path);
    }

    /**
     * Метод, отвечающий за проверку наличия пути к файлу
     *
     * @return
     */
    @Override
    public boolean IsPathExists() {
        return _file.exists() && !_file.isDirectory();
    }

    /**
     * Метод, отвечающий за создание пути к файлу
     */
    @Override
    public void CreatePath() {
        _file.getParentFile().mkdirs();
    }

    /**
     * Метод, отвечающий за создание пустого файла
     */
    @Override
    public void CreateFile() {
        CreatePath();
        try {
            _file.createNewFile();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Метод, отвечающий за запись данных в файл
     *
     * @param text
     */
    @Override
    public void Write(String text) {
        CheckFile();
        try {
            PrintWriter writer = new PrintWriter(this._path, "UTF-8");
            writer.println(text);
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Метод, отвечающий за запись данных в файл
     *
     * @param bytes
     */
    @Override
    public void Write(byte[] bytes) {
        CheckFile();
        try {
            FileOutputStream fos = new FileOutputStream(this._path);
            fos.write(bytes);
            fos.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Метод, отвечающий за чтение данных из файла
     *
     * @return строка с данными
     */
    @Override
    public String ReadString() {
        byte[] bytes = ReadBytes();
        try {
            return bytes != null ? new String(bytes, "UTF-8") : null;
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Метод, отвечающий за чтение данных из файла
     *
     * @return набор байтов
     */
    @Override
    public byte[] ReadBytes() {
        try {
            FileInputStream fis = new FileInputStream(_file);
            byte[] data = new byte[(int) _file.length()];
            fis.read(data);
            fis.close();
            return data;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
