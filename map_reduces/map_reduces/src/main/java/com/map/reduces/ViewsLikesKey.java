package com.map.reduces;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class ViewsLikesKey implements WritableComparable<ViewsLikesKey> {

    private Text videoId;
    private LongWritable views;
    private LongWritable likes;

    public ViewsLikesKey() {
        this.videoId = new Text();
        this.views = new LongWritable();
        this.likes = new LongWritable();
    }

    public ViewsLikesKey(Text videoId, LongWritable views, LongWritable likes) {
        this.videoId = videoId;
        this.views = views;
        this.likes = likes;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        videoId.readFields(in);
        views.readFields(in);
        likes.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        videoId.write(out);
        views.write(out);
        likes.write(out);
    }

    @Override
    public int compareTo(ViewsLikesKey other) {
        int result = views.compareTo(other.views);
        if (result == 0) {
            result = likes.compareTo(other.likes);
        }
        return result;
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
        return videoId.toString() + "\t" + views.toString() + "\t" + likes.toString();
    }

}
