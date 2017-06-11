package models.entity;

import java.io.Serializable;
import java.util.List;

/**
 * Модель данных а документе, хранящая текст документа и входящие слова
 */
public class DocumentData implements Serializable {
    public String Text;
    public List<String> Words;

    public DocumentData(String text, List<String> words) {
        Text = text;
        Words = words;
    }
}
