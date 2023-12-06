package whu.edu.cn.trajectory.db.query.basic.condition;

/**
 * @author xuqi
 * @date 2023/12/01
 */
public class IDQueryCondition extends AbstractQueryCondition{

    String moid;

    public IDQueryCondition(String moid) {
        this.moid = moid;
    }

    public String getMoid() {
        return moid;
    }

    @Override
    public String getConditionInfo() {
        return "IDQueryCondition{" +
                "moid='" + moid + '\'' +
                '}';
    }
}
