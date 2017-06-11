package io;

/**
 * Фабрика для создания FileManager
 */
public class FileManagerFactory {
    /**
     * Создать FileManager по указанной файловой системе
     * @param path Путь к файлу
     * @param fileSystemType Файловая система
     * @return Файловый менеджер
     */
    public static FileManager CreateByFS(String path, FileSystemTypeEnum fileSystemType){
        switch (fileSystemType){
            case WindowsFS:
                return new WindowsFileManager(path);
            case HDFS:
                return new HdfsFileManager(path);
            default:
                return null;
        }
    }

    /**
     * Создать FileManager по текущей файловой системе
     * @param path Путь к файлу
     * @return Файловый менеджер
     */
    public static FileManager CreateByCurrentFS(String path){
        FileSystemTypeEnum currentFS = FSConfiguration.Instance.FileSystemType; //получаем текущую файловую систему из настроек
        return CreateByFS(path, currentFS);
    }
}
