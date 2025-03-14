package com.map.reduces;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

// El objeto de la clave compuesta

public class TrendingVideoKey implements WritableComparable<TrendingVideoKey> {
    private Text trendingDate;
    private LongWritable views;
    private LongWritable likes;
    private Text videoId;

    public TrendingVideoKey() {
        this.trendingDate = new Text();
        this.views = new LongWritable();
        this.likes = new LongWritable();
        this.videoId = new Text();
    }

    public TrendingVideoKey(String trendingDate, long views, long likes, String videoId) {
        this.trendingDate = new Text(trendingDate);
        this.views = new LongWritable(views);
        this.likes = new LongWritable(likes);
        this.videoId = new Text(videoId);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        trendingDate.write(out);
        views.write(out);
        likes.write(out);
        videoId.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        trendingDate.readFields(in);
        views.readFields(in);
        likes.readFields(in);
        videoId.readFields(in);
    }

    @Override
    public int compareTo(TrendingVideoKey other) {

        // comprobamos las fechas
        int result = trendingDate.compareTo(other.trendingDate);

        // si son distintas listo, ordenamos de menor a mayor
        if (result != 0) {
            return result;
        }

        // Si llego aquí es que las fechas son las mismas, entonces comparamos las
        // vistas
        result = -views.compareTo(other.views);

        // si son distintas listo, ordenamos de mayor a menor (por eso el - de arriba)
        if (result != 0) {
            return result;
        }

        // si llego aquí es que las vistas son las mismas, entonces comparamos los likes
        result = -likes.compareTo(other.likes);

        if (result != 0) {
            return result;
        }

        // si llego aquí es que las vistas y los likes son los mismos, entonces
        // comparamos los videoIds
        result = videoId.compareTo(other.videoId);

        // apartir de aquí ya da igual pues, no podemos ordenar más
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        TrendingVideoKey that = (TrendingVideoKey) o;

        if (!trendingDate.equals(that.trendingDate))
            return false;
        if (!views.equals(that.views))
            return false;
        if (!likes.equals(that.likes))
            return false;
        return videoId.equals(that.videoId);
    }

    @Override
    public int hashCode() {
        int result = trendingDate.hashCode();
        result = 31 * result + views.hashCode();
        result = 31 * result + likes.hashCode();
        result = 31 * result + videoId.hashCode();
        return result;
    }

    public Text getTrendingDate() {
        return trendingDate;
    }

    public Text getVideoId() {
        return videoId;
    }

    public LongWritable getViews() {
        return views;
    }

    public LongWritable getLikes() {
        return likes;
    }

    @Override
    public String toString() {
        return trendingDate + "¶" + views + "¶" + likes + "¶" + videoId;
    }
}