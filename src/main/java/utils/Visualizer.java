package utils;

import de.erichseifert.gral.data.DataTable;
import de.erichseifert.gral.graphics.DrawingContext;
import de.erichseifert.gral.io.plots.DrawableWriter;
import de.erichseifert.gral.io.plots.DrawableWriterFactory;
import de.erichseifert.gral.plots.Plot;
import de.erichseifert.gral.plots.XYPlot;
import de.erichseifert.gral.plots.colors.RandomColors;
import de.erichseifert.gral.plots.points.DefaultPointRenderer2D;
import de.erichseifert.gral.plots.points.PointRenderer;
import de.erichseifert.gral.util.MathUtils;
import io.FileManager;
import models.entity.SimpleClusterData;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;

import java.awt.*;
import java.awt.geom.Ellipse2D;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

/**
 * Визуализатор данных кластеризации
 */
public class Visualizer {

    public static void Plot(FileManager fileManager, JavaRDD<Vector> tfidf, JavaRDD<Integer> clustersNumbers, Vector[] clustersCentres){
        //Получаем PCA модели
        Vector[] centres = PcaUtils.Process(clustersCentres);
        JavaRDD<Vector> pca = PcaUtils.Process(tfidf);

        //Маппим данные в модель
        JavaRDD<SimpleClusterData> model = ClusterDataMapper.GetSimpleClusterData(pca, clustersNumbers, centres);

        //Формируем DataTable под точки кластеров
        JavaRDD<DataTable> rdd1 = model.map(m -> {
            DataTable table = new DataTable(Double.class, Double.class);
            m.Items.forEach(i -> {
                double[] arr = i.toArray();
                table.add(arr[0], arr[1]);
            });
            return table;
        });

        List<DataTable> list1 = rdd1.collect();

        //Формируем DataTable под центры кластеров
        JavaRDD<DataTable> rdd2 = model.map(m -> {
            DataTable table = new DataTable(Double.class, Double.class);
            double[] arr = m.Center.toArray();
            table.add(arr[0], arr[1]);
            return table;
        });

        List<DataTable> list2 = rdd2.collect();

        //Совмещаем все точки
        List<DataTable> list = new LinkedList<>();
        list.addAll(list1);
        list.addAll(list2);

        DataTable[] arr = list.toArray(new DataTable[list.size()]);

        XYPlot plot = new XYPlot(arr);

        //Определяем рандомные цвета для класетров
        Color[] colors = getRandomColors(clustersCentres.length);

        //Указываем для каждой точки кластеров их цвет на диаграмме
        for (int i = 0; i < list1.size(); i++) {
            Color color = colors[i];
            PointRenderer pointRenderer = new DefaultPointRenderer2D();
            pointRenderer.setColor(color);
            plot.setPointRenderers(list1.get(i), pointRenderer);
        }

        //Указываем для центров кластеров их цвета и специальную форму
        for (int i = 0; i < list2.size(); i++) {
            Color color = colors[i];
            PointRenderer pointRenderer = new DefaultPointRenderer2D();
            pointRenderer.setColor(color);
            pointRenderer.setShape(new Ellipse2D.Double(-10.0, -10.0, 20.0, 20.0));
            plot.setPointRenderers(list2.get(i), pointRenderer);
        }

        try {
            //Выводим в изображение
            fileManager.Write(getJpg(plot));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Возвращает указанное количество случайных цветов
     * @param count Количество цветов
     * @return Случайные цвета
     */
    private static Color[] getRandomColors(int count) {
        Color[] res = new Color[count];
        for (int i = 0; i < res.length; i++) {
            res[i] = getRandomColor();
        }
        return res;
    }

    /**
     * Возвращает случаный цвет
     * @return Случайный цвет
     */
    private static Color getRandomColor() {
        Random random = new Random();
        float[] colorVariance =  new RandomColors().getColorVariance();
        float hue = colorVariance[0] + colorVariance[1]*random.nextFloat();
        float saturation = colorVariance[2] + colorVariance[3]*random.nextFloat();
        float brightness = colorVariance[4] + colorVariance[5]*random.nextFloat();
        return Color.getHSBColor(
            hue,
            MathUtils.limit(saturation, 0f, 1f),
            MathUtils.limit(brightness, 0f, 1f)
        );
    }

    /**
     * Формирует преобразует диаграмму в изображение
     * @param plot Диаграмма
     * @return Байтовый набор изображения
     * @throws IOException
     */
    private static byte[] getJpg(Plot plot) throws IOException {
        BufferedImage bImage = new BufferedImage(800, 600, BufferedImage.TYPE_INT_ARGB);
        Graphics2D g2d = (Graphics2D) bImage.getGraphics();
        DrawingContext context = new DrawingContext(g2d);
        plot.draw(context);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DrawableWriter wr = DrawableWriterFactory.getInstance().get("image/jpeg");
        wr.write(plot, baos, 800, 600);
        baos.flush();
        byte[] bytes = baos.toByteArray();
        baos.close();
        return bytes;
    }
}
