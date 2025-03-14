package com.map.reduces;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TrendingMetricsKey implements WritableComparable<TrendingMetricsKey> {
    private Text videoId;
    private Text trendingDate;
    private DoubleWritable likesToViewsRatio;
    private DoubleWritable dislikesToViewsRatio;
    private DoubleWritable commentsToViewsRatio;
    private DoubleWritable likesToDislikesRatio;

    public TrendingMetricsKey() {
        this.videoId = new Text();
        this.trendingDate = new Text();
        this.likesToViewsRatio = new DoubleWritable();
        this.dislikesToViewsRatio = new DoubleWritable();
        this.commentsToViewsRatio = new DoubleWritable();
        this.likesToDislikesRatio = new DoubleWritable();
    }

    public TrendingMetricsKey(String videoId, String trendingDate, double likesToViewsRatio,
            double dislikesToViewsRatio,
            double commentsToViewsRatio, double likesToDislikesRatio) {
        this.videoId = new Text(videoId);
        this.trendingDate = new Text(trendingDate);
        this.likesToViewsRatio = new DoubleWritable(likesToViewsRatio);
        this.dislikesToViewsRatio = new DoubleWritable(dislikesToViewsRatio);
        this.commentsToViewsRatio = new DoubleWritable(commentsToViewsRatio);
        this.likesToDislikesRatio = new DoubleWritable(likesToDislikesRatio);
    }

    public String getVideoId() {
        return videoId.toString();
    }

    public void setVideoId(String videoId) {
        this.videoId.set(videoId);
    }

    public String getTrendingDate() {
        return trendingDate.toString();
    }

    public void setTrendingDate(String trendingDate) {
        this.trendingDate.set(trendingDate);
    }

    public double getLikesToViewsRatio() {
        return likesToViewsRatio.get();
    }

    public void setLikesToViewsRatio(double likesToViewsRatio) {
        this.likesToViewsRatio.set(likesToViewsRatio);
    }

    public double getDislikesToViewsRatio() {
        return dislikesToViewsRatio.get();
    }

    public void setDislikesToViewsRatio(double dislikesToViewsRatio) {
        this.dislikesToViewsRatio.set(dislikesToViewsRatio);
    }

    public double getCommentsToViewsRatio() {
        return commentsToViewsRatio.get();
    }

    public void setCommentsToViewsRatio(double commentsToViewsRatio) {
        this.commentsToViewsRatio.set(commentsToViewsRatio);
    }

    public double getLikesToDislikesRatio() {
        return likesToDislikesRatio.get();
    }

    public void setLikesToDislikesRatio(double likesToDislikesRatio) {
        this.likesToDislikesRatio.set(likesToDislikesRatio);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        videoId.write(out);
        trendingDate.write(out);
        likesToViewsRatio.write(out);
        dislikesToViewsRatio.write(out);
        commentsToViewsRatio.write(out);
        likesToDislikesRatio.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        videoId.readFields(in);
        trendingDate.readFields(in);
        likesToViewsRatio.readFields(in);
        dislikesToViewsRatio.readFields(in);
        commentsToViewsRatio.readFields(in);
        likesToDislikesRatio.readFields(in);
    }

    @Override
    public int compareTo(TrendingMetricsKey o) {
        int result = trendingDate.compareTo(o.trendingDate);
        if (result == 0) {
            result = videoId.compareTo(o.videoId);
            if (result == 0) {
                result = -likesToViewsRatio.compareTo(o.likesToViewsRatio);
                if (result == 0) {
                    result = -dislikesToViewsRatio.compareTo(o.dislikesToViewsRatio);
                    if (result == 0) {
                        result = -commentsToViewsRatio.compareTo(o.commentsToViewsRatio);
                        if (result == 0) {
                            result = -likesToDislikesRatio.compareTo(o.likesToDislikesRatio);
                        }
                    }
                }
            }
        }
        return result;
    }

    @Override
    public String toString() {
        return videoId + "\t" + trendingDate + "\t" + likesToViewsRatio + "\t" + dislikesToViewsRatio + "\t"
                + commentsToViewsRatio
                + "\t" + likesToDislikesRatio;
    }
}
