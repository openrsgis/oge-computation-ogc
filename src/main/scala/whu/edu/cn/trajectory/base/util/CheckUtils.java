package whu.edu.cn.trajectory.base.util;

import org.apache.commons.lang.NullArgumentException;

import java.util.Collection;

/**
 * @author xuqi
 * @date 2023/11/06
 */
public class CheckUtils {
    public CheckUtils() {
    }

    public static void checkEmpty(Object... o) {
        int n = o.length;

        for (int i = 0; i < n; ++i) {
            if (o[i] == null) {
                throw new NullArgumentException("The parameter can not be null!");
            }
        }

    }

    public static boolean isCollectionEmpty(Collection var0) {
        return var0 == null || var0.isEmpty();
    }
}
