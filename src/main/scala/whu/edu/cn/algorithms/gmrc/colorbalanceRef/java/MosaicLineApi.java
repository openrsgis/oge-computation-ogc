package whu.edu.cn.algorithms.gmrc.colorbalanceRef.java;

import com.sun.jna.Library;
import com.sun.jna.Native;

public interface MosaicLineApi extends Library {
//    MosaicLineApi instance = Native.load(".\\lib\\dll\\mosaicline\\MosaicLine.dll", MosaicLineApi.class);
    MosaicLineApi instance = Native.load("./lib/dll/mosaicline/libMosaicLine.so", MosaicLineApi.class);
    boolean GenerateMosaicLine(String[] files, String sOuputDir);
}
