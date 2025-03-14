package com.map.reduces;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

// El objeto de la clave compuesta

public class TrendingDateAndIdVideoKey implements WritableComparable<TrendingDateAndIdVideoKey> {
    private Text trendingDate;
    private Text videoId;

    public TrendingDateAndIdVideoKey() {
        this.trendingDate = new Text();
        this.videoId = new Text();
    }

    public TrendingDateAndIdVideoKey(String trendingDate, String videoId) {
        this.trendingDate = new Text(trendingDate);
        this.videoId = new Text(videoId);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        trendingDate.write(out);
        videoId.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        trendingDate.readFields(in);
        videoId.readFields(in);
    }

    @Override
    public int compareTo(TrendingDateAndIdVideoKey other) {

        // comprobamos las fechas
        int result = trendingDate.compareTo(other.trendingDate);

        // si son distintas listo, ordenamos de menor a mayor
        if (result != 0) {
            return result;
        }

        // si llego aquí es que las fechas son iguales
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

        TrendingDateAndIdVideoKey that = (TrendingDateAndIdVideoKey) o;

        if (!trendingDate.equals(that.trendingDate))
            return false;
        return videoId.equals(that.videoId);
    }

    @Override
    public int hashCode() {
        int result = trendingDate.hashCode();
        result = 31 * result + videoId.hashCode();
        return result;
    }

    public Text getTrendingDate() {
        return trendingDate;
    }

    public Text getVideoId() {
        return videoId;
    }

    @Override
    public String toString() {
        return trendingDate + "¶" + videoId;
    }
}