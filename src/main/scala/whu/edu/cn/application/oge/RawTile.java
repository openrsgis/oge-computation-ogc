package whu.edu.cn.application.oge;

import geotrellis.raster.Tile;

import java.io.Serializable;

public class RawTile implements Serializable {
        String path;
        String time;
        String measurement;
        String product;
        double[] p_bottom_left = new double[2];/* extent of the tile*/
        double[] p_upper_right = new double[2];
        int[] row_col = new int[2];/* ranks of the tile*/
        long[] offset = new long[2];/* offsets of the tile*/
        byte[] tileBuf;/* byte stream of the tile*/
//        int bitPerSample;/* bit of the tile*/
        int level;/* level of the overview*/
        double rotation;
        double resolution;
        int crs;
        String dType;
        Tile tile;

        public String getPath() {
            return this.path;
        }

        public String getTime() {
            return this.time;
        }

        public String getMeasurement() {
            return this.measurement;
        }

        public String getProduct() {
            return this.product;
        }

        public double[] getP_bottom_left() {
            return this.p_bottom_left;
        }

        public double getP_bottom_leftX() {
            return this.p_bottom_left[0];
        }

        public double getP_bottom_leftY() {
            return this.p_bottom_left[1];
        }

        public double[] getP_upper_right() {
            return this.p_upper_right;
        }

        public double getP_upper_rightX() {
            return this.p_upper_right[0];
        }

        public double getP_upper_rightY() {
            return this.p_upper_right[1];
        }

        public int[] getRow_col() {
            return this.row_col;
        }

        public int getRow() {
            return this.row_col[0];
        }

        public int getCol() {
            return this.row_col[1];
        }

        public long[] getOffset() {
            return this.offset;
        }

        public byte[] getTileBuf() {
            return this.tileBuf;
        }

//        public int getBitPerSample() {
//            return this.bitPerSample;
//        }

        public int getLevel() {
            return this.level;
        }

        public double getRotation() {
            return this.rotation;
        }

        public double getResolution() {
            return this.resolution;
        }

        public int getCrs() {
            return this.crs;
        }

        public String getDType() {
            return this.dType;
        }

        public Tile getTile() {
            return this.tile;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public void setTime(String time) {
            this.time = time;
        }

        public void setMeasurement(String measurement) {
            this.measurement = measurement;
        }

        public void setProduct(String product) {
            this.product = product;
        }

        public void setP_bottom_left(double[] p_bottom_left) {
            this.p_bottom_left = p_bottom_left;
        }

        public void setP_upper_right(double[] p_upper_right) {
            this.p_upper_right = p_upper_right;
        }

        public void setRow_col(int[] row_col) {
            this.row_col = row_col;
        }

        public void setRow(int row) {
            this.row_col[0] = row;
        }

        public void setCol(int col) {
            this.row_col[1] = col;
        }

        public void setOffset(long[] offset) {
            this.offset = offset;
        }

        public void setTileBuf(byte[] tileBuf) {
            this.tileBuf = tileBuf;
        }

//        public void setBitPerSample(int bitPerSample) {
//            this.bitPerSample = bitPerSample;
//        }

        public void setLevel(int level) {
            this.level = level;
        }

        public void setRotation(double rotation) {
            this.rotation = rotation;
        }

        public void setResolution(double resolution) {
            this.resolution = resolution;
        }

        public void setCrs(int crs) {
            this.crs = crs;
        }

        public void setDType(String dType) {
            this.dType = dType;
        }

        public void setTile(Tile tile) {
            this.tile = tile;
        }
    }