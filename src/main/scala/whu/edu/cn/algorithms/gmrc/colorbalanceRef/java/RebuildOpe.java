package whu.edu.cn.algorithms.gmrc.colorbalanceRef.java;

public class RebuildOpe {

    short VALID_VALUE = 1;

    private int mLevel;
    int[] mSize;
    float[] mWeight;
    int mHeight, mWidth;

    public int getmWidth() {
        return mWidth;
    }

    short[][] GSr;
    short[][] GSg;
    short[][] GSb;
    short[][] Vr;
    short[][] Vg;
    short[][] Vb;
    short[][] LPr;
    short[][] LPg;
    short[][] LPb;
    short[][] EXPr;
    short[][] EXPg;
    short[][] EXPb;
    boolean[] mBlackArr;

    public short[] mBuf;
    public short[] mWallisBuf;

    public RebuildOpe(int level, int height, int width)
    {
        mLevel = level;
        mHeight = height;
        mWidth = width;
        mSize = new int[2 * (level + 1)];
        int th = height;
        int tw = width;
        for (int k = 0; k<=level; k++)
        {
            mSize[2*k] = th;
            mSize[2*k + 1] = tw;
            th= (th-1)/2;
            tw= (tw-1)/2;
        }
        mBlackArr = new boolean[height * width * 3];
        CommonTools.ClearBuf(mBlackArr, true);
        mWeight = new float[5];
        for(int i=0; i<5; i++)
        {
            if(Math.abs((int)(i- 2))== 2) mWeight[i]= 1.0f/16;
            else if(Math.abs((int)(i- 2))== 1) mWeight[i]= 1.0f/4;//设置权重  虚拟矩阵
            else mWeight[i]= 3.0f/8;
        }
        GSr = new short[level + 1][];
        GSg = new short[level + 1][];
        GSb = new short[level + 1][];

        LPr = new short[level + 1][];
        LPg = new short[level + 1][];
        LPb = new short[level + 1][];
        for (int i=0; i<=level; i++)
        {
            GSr[i]= new short[(mSize[2*i]*mSize[2*i+1])];
            GSg[i]= new short[(mSize[2*i]*mSize[2*i+1])];
            GSb[i]= new short[(mSize[2*i]*mSize[2*i+1])];

            LPr[i]= new short[(mSize[2*i]*mSize[2*i+1])];
            LPg[i]= new short[(mSize[2*i]*mSize[2*i+1])];
            LPb[i]= new short[(mSize[2*i]*mSize[2*i+1])];
        }
        Vr = new short[level][];
        Vg = new short[level][];
        Vb = new short[level][];
        for(int k=0; k<level; k++)
        {
            Vr[k]= new short[(mSize[2*k] + 3)*(mSize[2*k+ 1] + 3)];//每一层都多了三行三列
            Vg[k]= new short[(mSize[2*k] + 3)*(mSize[2*k+ 1] + 3)];
            Vb[k]= new short[(mSize[2*k] + 3)*(mSize[2*k+ 1] + 3)];
        }

        EXPr = new short[level + 1][];
        EXPg = new short[level + 1][];
        EXPb = new short[level + 1][];
        for(int k=0; k<=level; k++)
        {
            EXPr[k]= new short[(mSize[2*k])*(mSize[2*k+ 1])];
            EXPg[k]= new short[(mSize[2*k])*(mSize[2*k+ 1])];
            EXPb[k]= new short[(mSize[2*k])*(mSize[2*k+ 1])];
        }
    }

    public void SetBufData(short[] pBuf, int height, int width)
    {
        mHeight = height;
        mWidth = width;
        int th = height;
        int tw = width;
        for (int k = 0; k<=mLevel; k++)
        {
            mSize[2*k]= th;
            mSize[2*k + 1]= tw;
            th= (th-1)/2;
            tw= (tw-1)/2;
        }
        mBuf = pBuf;
    }

    public boolean GaussianModel_Parallel()
    {
        int nChannel = 3;
        int index;
        int k;

        int height, width;
        /////////////////////////////////////////高斯金字塔层赋值
        int border = mSize[0]*mSize[1];
        for (index =0; index < border; index++)
        {
            int i = index / mSize[1];
            int j = index % mSize[1];
            GSr[0][i*mSize[1]+ j] = mBuf[ i*mSize[1]*nChannel + nChannel*j ];
            GSg[0][i*mSize[1]+ j] = mBuf[ i*mSize[1]*nChannel + nChannel*j + 1];
            GSb[0][i*mSize[1]+ j] = mBuf[ + i*mSize[1]*nChannel + nChannel*j + 2];//底层赋值
        }

        ////////////////////////////////////////////高斯金字塔的层层赋值
        for (k=1; k<=mLevel; k++)
        {
            height = mSize[2 * (k - 1)] + 2;//mSize是根据原始影像的大小计算得来的
            width = mSize[2 * (k - 1) + 1] + 2;//每层的高斯都往外扩两行，为了处理边边角角的像元

            border = mSize[2 * (k - 1)] * mSize[2 * (k - 1) + 1];
            for (index = 0; index < border; index++) {
                int i = index / mSize[2 * (k - 1) + 1];
                int j = index % mSize[2 * (k - 1) + 1];
                Vr[k - 1][(i + 2) * width + j + 2] = GSr[k - 1][i * mSize[2 * (k - 1) + 1] + j];//GSr[0]已经赋值过了
                Vg[k - 1][(i + 2) * width + j + 2] = GSg[k - 1][i * mSize[2 * (k - 1) + 1] + j];
                Vb[k - 1][(i + 2) * width + j + 2] = GSb[k - 1][i * mSize[2 * (k - 1) + 1] + j];
            }
            for (int i = 0; i < mSize[2 * (k - 1)]; i++) {
                Vr[k - 1][i * width + 1] = (short) Math.max(2 * Vr[k - 1][i * width + 2] - Vr[k - 1][i * width + 3], 0);
                Vg[k - 1][i * width + 1] = (short) Math.max(2 * Vg[k - 1][i * width + 2] - Vg[k - 1][i * width + 3], 0);
                Vb[k - 1][i * width + 1] = (short) Math.max(2 * Vb[k - 1][i * width + 2] - Vb[k - 1][i * width + 3], 0);

                Vr[k - 1][i * width + 0] = (short) Math.max(2 * Vr[k - 1][i * width + 2] - Vr[k - 1][i * width + 4], 0);
                Vg[k - 1][i * width + 0] = (short) Math.max(2 * Vg[k - 1][i * width + 2] - Vg[k - 1][i * width + 4], 0);
                Vb[k - 1][i * width + 0] = (short) Math.max(2 * Vb[k - 1][i * width + 2] - Vb[k - 1][i * width + 4], 0);
            }
            for (int j = 0; j < width; j++) {
                Vr[k - 1][1 * width + j] = (short) Math.max(2 * Vr[k - 1][2 * width + j] - Vr[k - 1][3 * width + j], 0);
                Vg[k - 1][1 * width + j] = (short) Math.max(2 * Vg[k - 1][2 * width + j] - Vg[k - 1][3 * width + j], 0);
                Vb[k - 1][1 * width + j] = (short) Math.max(2 * Vb[k - 1][2 * width + j] - Vb[k - 1][3 * width + j], 0);

                Vr[k - 1][0 * width + j] = (short) Math.max(2 * Vr[k - 1][2 * width + j] - Vr[k - 1][4 * width + j], 0);
                Vg[k - 1][0 * width + j] = (short) Math.max(2 * Vg[k - 1][2 * width + j] - Vg[k - 1][4 * width + j], 0);
                Vb[k - 1][0 * width + j] = (short) Math.max(2 * Vb[k - 1][2 * width + j] - Vb[k - 1][4 * width + j], 0);
            }

            IM_DownSample8U_C1(Vr[k - 1], GSr[k], width, height, width, mSize[2 * k + 1], mSize[2 * k], mSize[2 * k + 1], 1);
            IM_DownSample8U_C1(Vg[k - 1], GSg[k], width, height, width, mSize[2 * k + 1], mSize[2 * k], mSize[2 * k + 1], 1);
            IM_DownSample8U_C1(Vb[k - 1], GSb[k], width, height, width, mSize[2 * k + 1], mSize[2 * k], mSize[2 * k + 1], 1);

        }
        border = mSize[2*mLevel] * mSize[2*mLevel + 1];
        mWallisBuf = new short[border * 3];

        for (index = 0; index < border; index++)
        {
            int i = index / mSize[2 * mLevel + 1];
            int j = index % mSize[2 * mLevel + 1];
            mWallisBuf[i * mSize[2 * mLevel + 1] * 3 + 3*j] = GSr[mLevel][i * mSize[2 * mLevel + 1]+ j];
            mWallisBuf[i * mSize[2 * mLevel + 1] * 3 + 3*j + 1] = GSg[mLevel][i * mSize[2 * mLevel + 1] + j];
            mWallisBuf[i * mSize[2 * mLevel + 1] * 3 + 3*j + 2] = GSb[mLevel][i * mSize[2 * mLevel + 1] + j];
        }

        return true;
    }

    public boolean ColorMap(int[] colorMap)
    {
        int MAX_LEVEL = 256;
        short red, green, blue, newR, newG, newB;
        int height= mSize[2*mLevel];
        int width = mSize[2*mLevel + 1];
        for (int m=0; m<height; m++)
        {
            for (int n=0; n<width; n++)
            {
                red = mWallisBuf[m * width * 3 + 3 * n];
                green = mWallisBuf[m * width * 3 + 3 * n + 1];
                blue = mWallisBuf[m * width * 3 + 3 * n + 2];

                newR = (short) colorMap[red];
                newG = (short)colorMap[green + MAX_LEVEL];
                newB = (short)colorMap[blue + MAX_LEVEL * 2];
                mWallisBuf[m * width * 3 + 3 * n] = newR;
                mWallisBuf[m * width * 3 + 3 * n + 1] = newG;
                mWallisBuf[m * width * 3 + 3 * n + 2] = newB;
            }
        }

        return true;
    }

    public boolean LaplacianManage()
    {
        int k, m, i, j, n;
        int height, width;
        //////////////////////////////////////////////////////////////////////////虚拟矩阵赋值
        for (k=0; k<mLevel; k++)
        {
            height = mSize[2*(k + 1)] + 3;
            width = mSize[2*(k + 1) + 1] + 3;
            int index;
            int border = mSize[2*(k + 1)] * mSize[2*(k + 1) + 1];

            for (index =0; index < border; index++)
            {
                i = index / mSize[2*(k + 1) + 1];
                j = index % mSize[2*(k + 1) + 1];
                Vr[k][(i + 1)*width + j + 1] = GSr[k + 1][i*mSize[2*(k + 1) + 1] + j];//高斯的上一层赋给这一层的拉普拉斯
                Vg[k][(i + 1)*width + j + 1] = GSg[k + 1][i*mSize[2*(k + 1) + 1] + j];
                Vb[k][(i + 1)*width + j + 1] = GSb[k + 1][i*mSize[2*(k + 1) + 1] + j];
            }



            for (j=1, m=1; m<2; m++)
            {
                for (i=0; i<height-3; i++)
                {
                    Vr[k][i*width + j - m] = (short) Math.max(2*Vr[k][i*width + j] - Vr[k][i*width + j + m], 0);//扩充矩阵
                    Vg[k][i*width + j - m] = (short) Math.max(2*Vg[k][i*width + j] - Vg[k][i*width + j + m], 0);
                    Vb[k][i*width + j - m] = (short) Math.max(2*Vb[k][i*width + j] - Vb[k][i*width + j + m], 0);
                }
            }

            for (i=1, m=1; m<2; m++)
            {
                for (j=0; j<width-2; j++)
                {
                    Vr[k][(i - m)*width + j] = (short) Math.max(2*Vr[k][i*width  +j] - Vr[k][(i + m)*width + j], 0);
                    Vg[k][(i - m)*width + j] = (short) Math.max(2*Vg[k][i*width  +j] - Vg[k][(i + m)*width + j], 0);
                    Vb[k][(i - m)*width + j] = (short) Math.max(2*Vb[k][i*width  +j] - Vb[k][(i + m)*width + j], 0);
                }
            }

            for (j=width-3, m=1; m<=2; m++)
            {
                for (i=0; i<height-2; i++)
                {
                    Vr[k][i*width + j + m] = (short) Math.max(2*Vr[k][i*width + j] - Vr[k][i*width + j - m], 0);
                    Vg[k][i*width + j + m] = (short) Math.max(2*Vg[k][i*width + j] - Vg[k][i*width + j - m], 0);
                    Vb[k][i*width + j + m] = (short) Math.max(2*Vb[k][i*width + j] - Vb[k][i*width + j - m], 0);
                }
            }

            for (i=height-3,m=1; m<=2; m++)
            {
                for (j=0; j<width; j++)
                {
                    Vr[k][(i + m)*width + j] = (short) Math.max(2*Vr[k][i*width + j] - Vr[k][(i - m)*width + j], 0);
                    Vg[k][(i + m)*width + j] = (short) Math.max(2*Vg[k][i*width + j] - Vg[k][(i - m)*width + j], 0);
                    Vb[k][(i + m)*width + j] = (short) Math.max(2*Vb[k][i*width + j] - Vb[k][(i - m)*width + j], 0);
                }
            }

            border = mSize[2*k] * mSize[2*k + 1];
            for (index =0; index < border; index++)
            {

                i = index / mSize[2*k + 1];
                j = index % mSize[2*k + 1];
                float t1 = 0.0f;
                float t2 = 0.0f;
                float t3 = 0.0f;
                int indi;
                for (indi =0; indi < 25; indi++)
                {
                    m = indi / 5;
                    n = indi % 5;
                    if(((i+ m) % 2 == 0) && ((j+ n) % 2 == 0))
                    {
                        t1 += 1.0f * Vr[k][((i + m - 2)/2 + 1)*width + (j + n - 2)/2 + 1] * mWeight[m] * mWeight[n];
                        t2 += 1.0f * Vg[k][((i+ m - 2)/2 + 1)*width + (j+ n - 2)/2 + 1] * mWeight[m] * mWeight[n];
                        t3 += 1.0f * Vb[k][((i+ m - 2)/2 + 1)*width + (j+ n - 2)/2 + 1] * mWeight[m] * mWeight[n];
                    }
                }
                t1 = 4*t1;
                t2 = 4*t2;
                t3 = 4*t3;
                EXPr[k][i*mSize[2*k+ 1]+ j] = (short)t1;
                EXPg[k][i*mSize[2*k+ 1]+ j] = (short)t2;
                EXPb[k][i*mSize[2*k+ 1]+ j] = (short)t3;
            }

        }

        int index;
        int border = mSize[2*mLevel] * mSize[2*mLevel + 1];
        for (index =0; index < border; index++)
        {
            i= index / mSize[2*mLevel + 1];
            j = index % mSize[2*mLevel + 1];
            EXPr[mLevel][i * mSize[2*mLevel+ 1]+ j] = 0;
            EXPg[mLevel][i * mSize[2*mLevel+ 1]+ j] = 0;
            EXPb[mLevel][i * mSize[2*mLevel+ 1]+ j] = 0;
        }



        for (k=0; k<mLevel+ 1; k++)
        {
            border = mSize[2*k] * mSize[2*k + 1];
            for (index =0; index < border; index++)
            {
                i= index / mSize[2*k + 1];
                j = index % mSize[2*k + 1];
                LPr[k][i * mSize[2*k+ 1]+ j] = (short) (GSr[k][i * mSize[2*k+ 1]+ j] - EXPr[k][i * mSize[2*k+ 1]+ j]);
                LPg[k][i * mSize[2*k+ 1]+ j] = (short) (GSg[k][i * mSize[2*k+ 1]+ j] - EXPg[k][i * mSize[2*k+ 1]+ j]);
                LPb[k][i * mSize[2*k+ 1]+ j] = (short) (GSb[k][i * mSize[2*k+ 1]+ j] - EXPb[k][i * mSize[2*k+ 1]+ j]);
            }

        }

        return true;
    }

    public boolean BuildImage()
    {
        int index;
        int border = 0;

        int i, j ,k , m, n, width, height;

        //统计原始影像零值位置
        CommonTools.ClearBuf(mBlackArr, true);
        for (i=0; i<mSize[0]; i++)
        {
            for (j=0; j<mSize[1]; j++)
            {
                if (0 == GSr[0][i*mSize[1] + j] && 0 == GSg[0][i*mSize[1] + j] && 0 == GSb[0][i*mSize[1] + j])
                {
                    mBlackArr[i*mSize[1] + j] = false;
                }
            }
        }
        for (i=0; i<mSize[2*mLevel]; i++)
        {
            for (j=0; j<mSize[2*mLevel+1]; j++)
            {
                GSr[mLevel][i*mSize[2*mLevel+1] + j] = mWallisBuf[ i*mSize[2*mLevel+1]*3+j*3 ];
                GSg[mLevel][i*mSize[2*mLevel+1] + j] = mWallisBuf[ i*mSize[2*mLevel+1]*3+j*3 +1];
                GSb[mLevel][i*mSize[2*mLevel+1] + j] = mWallisBuf[ i*mSize[2*mLevel+1]*3+j*3 +2];
            }
        }

        for (k=mLevel-1; k>=0; k--)
        {	////////////////////////////////////////////////////////////////////////////////////////////////////////改动
            height = mSize[2*(k + 1)] + 3;
            width = mSize[2*(k + 1) + 1] + 3;
            border = mSize[2*(k + 1)] * mSize[2*(k + 1) + 1];
            for (index = 0; index < border; index++)
            {
                i= index / mSize[2*(k + 1) + 1];
                j = index % mSize[2*(k + 1) + 1];
                Vr[k][(i + 1)*width + j + 1] = GSr[k + 1][i*(width - 3) + j];
                Vg[k][(i + 1)*width + j + 1] = GSg[k + 1][i*(width - 3) + j];
                Vb[k][(i + 1)*width + j + 1] = GSb[k + 1][i*(width - 3) + j];
            }

            for (j=1, m=1; m<2; m++)
            {
                for (i=0; i<height-3; i++)
                {
                    Vr[k][i*width + j - m] = (short) Math.max(2*Vr[k][i*width + j] - Vr[k][i*width + j + m], 0);
                    Vg[k][i*width + j - m] = (short) Math.max(2*Vg[k][i*width + j] - Vg[k][i*width + j + m], 0);
                    Vb[k][i*width + j - m] = (short) Math.max(2*Vb[k][i*width + j] - Vb[k][i*width + j + m], 0);
                }
            }

            for (i=1, m=1; m<2; m++)
            {
                for (j=0; j<width-2; j++)
                {
                    Vr[k][(i - m)*width + j] = (short) Math.max(2*Vr[k][i*width  +j] - Vr[k][(i + m)*width + j], 0);
                    Vg[k][(i - m)*width + j] = (short) Math.max(2*Vg[k][i*width  +j] - Vg[k][(i + m)*width + j], 0);
                    Vb[k][(i - m)*width + j] = (short) Math.max(2*Vb[k][i*width  +j] - Vb[k][(i + m)*width + j], 0);
                }
            }

            for (j=width-3, m=1; m<=2; m++)
            {
                for (i=0; i<height-2; i++)
                {
                    Vr[k][i*width + j + m] = (short) Math.max(2*Vr[k][i*width + j] - Vr[k][i*width + j - m], 0);
                    Vg[k][i*width + j + m] = (short) Math.max(2*Vg[k][i*width + j] - Vg[k][i*width + j - m], 0);
                    Vb[k][i*width + j + m] = (short) Math.max(2*Vb[k][i*width + j] - Vb[k][i*width + j - m], 0);
                }
            }

            for (i=height-3,m=1; m<=2; m++)
            {
                for (j=0; j<width; j++)
                {
                    Vr[k][(i + m)*width + j] = (short) Math.max(2*Vr[k][i*width + j] - Vr[k][(i - m)*width + j], 0);
                    Vg[k][(i + m)*width + j] = (short) Math.max(2*Vg[k][i*width + j] - Vg[k][(i - m)*width + j], 0);
                    Vb[k][(i + m)*width + j] = (short) Math.max(2*Vb[k][i*width + j] - Vb[k][(i - m)*width + j], 0);
                }
            }

            border = mSize[2*k] * mSize[2*k + 1];

            for (index =0; index < border; index++)
            {

                i = index / mSize[2*k + 1];
                j = index % mSize[2*k + 1];
                float t1 = 0.0f;
                float t2 = 0.0f;
                float t3 = 0.0f;
                int indi;
                for (indi =0; indi < 25; indi++)
                {
                    m = indi / 5;
                    n = indi % 5;
                    if(((i+ m)%2== 0) && ((j+ n)%2== 0))
                    {
                        t1 += Vr[k][((i + m - 2)/2 + 1)*width + (j + n - 2)/2 + 1] * mWeight[m] * mWeight[n];
                        t2 += Vg[k][((i+ m - 2)/2 + 1)*width + (j+ n - 2)/2 + 1] * mWeight[m] * mWeight[n];
                        t3 += Vb[k][((i+ m - 2)/2 + 1)*width + (j+ n - 2)/2 + 1] * mWeight[m] * mWeight[n];
                    }
                }
                t1 = 4*t1;
                t2 = 4*t2;
                t3 = 4*t3;
                EXPr[k][i*mSize[2*k+ 1]+ j] = (short) t1;
                EXPg[k][i*mSize[2*k+ 1]+ j] = (short) t2;
                EXPb[k][i*mSize[2*k+ 1]+ j] = (short) t3;

                t1 = LPr[k][i * mSize[2*k+ 1]+ j] + EXPr[k][i * mSize[2*k+ 1]+ j];
                t2 = LPg[k][i * mSize[2*k+ 1]+ j] + EXPg[k][i * mSize[2*k+ 1]+ j];
                t3 = LPb[k][i * mSize[2*k+ 1]+ j] + EXPb[k][i * mSize[2*k+ 1]+ j];

                GSr[k][i * mSize[2*k+ 1]+ j] = (t1 < 0) ? 0 : ((t1 <= 255) ? (short) t1 : 255);
                GSg[k][i * mSize[2*k+ 1]+ j] = (t2 < 0) ? 0 : ((t2 <= 255) ? (short) t2 : 255);
                GSb[k][i * mSize[2*k+ 1]+ j] = (t3 < 0) ? 0 : ((t3 <= 255) ? (short) t3 : 255);
            }

        }
        for (i=0; i<mSize[0]; i++)
        {
            for (j=0; j<mSize[1]; j++)
            {
                if (0 == GSr[0][i*mSize[1] + j] && 0 == GSg[0][i*mSize[1] + j] && 0 == GSb[0][i*mSize[1] + j] &&
                        mBlackArr[i*mSize[1] + j])
                {
                    GSr[0][i*mSize[1] + j] = 1;
                    GSg[0][i*mSize[1] + j] = 1;
                    GSb[0][i*mSize[1] + j] = 1;

                }
            }
        }

        short red, green, blue;
        for (i =0; i<mSize[0]; i++)
        {
            for (j =0; j<mSize[1]; j++)
            {
                red = mBuf[i*mSize[1] * 3 + 3 * j];
                green = mBuf[i*mSize[1] * 3 + 3 * j + 1];
                blue = mBuf[i*mSize[1] * 3 + 3 * j + 2];
                if (red <= VALID_VALUE && green <= VALID_VALUE && blue <= VALID_VALUE)
                {
                    continue;
                }
                mBuf[i*mSize[1]*3 + 3*j] = GSr[0][i*mSize[1] + j];
                mBuf[i*mSize[1]*3 + 3*j + 1] = GSg[0][i*mSize[1] + j];
                mBuf[i*mSize[1]*3 + 3*j + 2] = GSb[0][i*mSize[1] + j];
            }
        }

        return true;
    }

    public void SetWallisBufData(short[] pBuf)
    {
        mWallisBuf = pBuf;
    }


    public int IM_DownSample8U_C1(short[]Src, short[] Dest, int SrcW, int SrcH, int StrideS, int DstW, int DstH, int StrideD, int Channel)
    {
        if ((Channel != 1) && (Channel != 3) && (Channel != 4))        return -1;
        if (Channel == 1)
        {
            int Sum1, Sum2, Sum3, Sum4, Sum5;
            for(int Y = 0; Y<DstH; Y++)
            {
                int LinePD = Y * StrideD;
                int P1 = (2 * Y) * StrideS;
                int P2 = P1 + StrideS;
                int P3 = P2 + StrideS;
                int P4 = P3 + StrideS;
                int P5 = P4 + StrideS;
                Sum3 = Src[P1 + 0] + ((Src[P2 + 0] + Src[P4 + 0]) << 2) + Src[P3 + 0] * 6 + Src[P5 + 0];
                Sum4 = Src[P1 + 1] + ((Src[P2 + 1] + Src[P4 + 1]) << 2) + Src[P3 + 1] * 6 + Src[P5 + 1];
                Sum5 = Src[P1 + 2] + ((Src[P2 + 2] + Src[P4 + 2]) << 2) + Src[P3 + 2] * 6 + Src[P5 + 2];
                for (int X = 0; X < DstW; X++ )
                {
                    Sum1 = Sum3;    Sum2 = Sum4;    Sum3 = Sum5;
                    Sum4 = Src[P1 + 3] + ((Src[P2 + 3] + Src[P4 + 3]) << 2) + Src[P3 + 3] * 6 + Src[P5 + 3];
                    Sum5 = Src[P1 + 4] + ((Src[P2 + 4] + Src[P4 + 4]) << 2) + Src[P3 + 4] * 6 + Src[P5 + 4];
                    Dest[LinePD + X] = (short) ((Sum1 + ((Sum2 + Sum4) << 2) + Sum3 * 6 + Sum5 + 128) >> 8);        //    注意四舍五入
                    P1 += 2;    P2 += 2;    P3 += 2;    P4 += 2;    P5 += 2;
                }
            }
        }
        else if( 3 == Channel)
        {
            int Sum1, Sum2, Sum3, Sum4, Sum5;
            for(int Y = 0; Y<DstH; Y++)
            {
                int LinePD = Y * StrideD;
                int P1 = (2 * Y) * StrideS;
                int P2 = P1 + StrideS;
                int P3 = P2 + StrideS;
                int P4 = P3 + StrideS;
                int P5 = P4 + StrideS;
                Sum3 = Src[P1 + 0] + ((Src[P2 + 0] + Src[P4 + 0]) << 2) + Src[P3 + 0] * 6 + Src[P5 + 0];
                Sum4 = Src[P1 + 1] + ((Src[P2 + 1] + Src[P4 + 1]) << 2) + Src[P3 + 1] * 6 + Src[P5 + 1];
                Sum5 = Src[P1 + 2] + ((Src[P2 + 2] + Src[P4 + 2]) << 2) + Src[P3 + 2] * 6 + Src[P5 + 2];
                for (int X = 0; X < StrideD; X++ )
                {
                    Sum1 = Sum3;    Sum2 = Sum4;    Sum3 = Sum5;
                    Sum4 = Src[P1 + 3] + ((Src[P2 + 3] + Src[P4 + 3]) << 2) + Src[P3 + 3] * 6 + Src[P5 + 3];
                    Sum5 = Src[P1 + 4] + ((Src[P2 + 4] + Src[P4 + 4]) << 2) + Src[P3 + 4] * 6 + Src[P5 + 4];
                    Dest[LinePD + X] = (short) ((Sum1 + ((Sum2 + Sum4) << 2) + Sum3 * 6 + Sum5 + 128) >> 8);        //    注意四舍五入
                    P1 += 2;    P2 += 2;    P3 += 2;    P4 += 2;    P5 += 2;
                }
            }
        }
        return 1;
    }


}