package whu.edu.cn.application.oge;

import geotrellis.raster.Tile;

import java.io.Serializable;

public class RawTile implements Serializable {
        private String path;
        private String time;
        private String measurement;
        private String product;
        private final double[] LngLatBottomLeft = new double[2];/* extent of the tile*/
        private final double[] LngLatUpperRight = new double[2];
        private final int[] rowCol = new int[2];/* ranks of the tile*/
        private final long[] offset = new long[2];/* offsets of the tile*/

        private byte[] tileBuf;/* byte stream of the tile*/
        private int level;/* level of the overview*/
        private double rotation;
        private double resolution;
        private int crs;
        private String dataType;
        private Tile tile;


    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getMeasurement() {
        return measurement;
    }

    public void setMeasurement(String measurement) {
        this.measurement = measurement;
    }

    public String getProduct() {
        return product;
    }

    public void setProduct(String product) {
        this.product = product;
    }

    public double[] getLngLatBottomLeft() {
        return LngLatBottomLeft;
    }

    public void setLngLatBottomLeft(double lngLatBottomLeft0, double lngLatBottomLeft1) {
        this.LngLatBottomLeft[0] = lngLatBottomLeft0;
        this.LngLatBottomLeft[1] = lngLatBottomLeft1;
    }

    public double[] getLngLatUpperRight() {
        return LngLatUpperRight;
    }

    public void setLngLatUpperRight(double lngLatUpperRight0, double lngLatUpperRight1) {
        this.LngLatUpperRight[0] = lngLatUpperRight0;
        this.LngLatUpperRight[1] = lngLatUpperRight1;
    }

    public int[] getRowCol() {
        return rowCol;
    }

    public void setRowCol(int row, int col) {
        this.rowCol[0] = row;
        this.rowCol[1] = col;
    }

    public int getRow(){
        return this.rowCol[0];
    }

    public int getCol(){
        return this.rowCol[1];
    }



    public long[] getOffset() {
        return offset;
    }

    public void setOffset(long offset0, long offset1) {
        this.offset[0] = offset0;
        this.offset[1] = offset1;
    }

    public byte[] getTileBuf() {
        return tileBuf;
    }

    public void setTileBuf(byte[] tileBuf) {
        this.tileBuf = tileBuf;
    }

    public int getLevel() {
        return level;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    public double getRotation() {
        return rotation;
    }

    public void setRotation(double rotation) {
        this.rotation = rotation;
    }

    public double getResolution() {
        return resolution;
    }

    public void setResolution(double resolution) {
        this.resolution = resolution;
    }

    public int getCrs() {
        return crs;
    }

    public void setCrs(int crs) {
        this.crs = crs;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public Tile getTile() {
        return tile;
    }

    public void setTile(Tile tile) {
        this.tile = tile;
    }
}