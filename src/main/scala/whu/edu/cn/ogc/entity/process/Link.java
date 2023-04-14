package whu.edu.cn.ogc.entity.process;

public class Link {
    private String type;
    private String rel;
    private String title;
    private String href;
    private String hreflang;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getRel() {
        return rel;
    }

    public void setRel(String rel) {
        this.rel = rel;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getHref() {
        return href;
    }

    public void setHref(String href) {
        this.href = href;
    }

    public String getHreflang() {
        return hreflang;
    }

    public void setHreflang(String hreflang) {
        this.hreflang = hreflang;
    }

    /**
     * 修补Link中的href,对于相对uri进行完善,添加http
     * @param baseUrl 基本url for excample http:125.220.153.26/api/   截止到collections之前的部分
     * @return Link完善后的Link
     */
    public Link fixLinkHref(String baseUrl){
        String href = this.href;
        // 首先判断是否是空缺的Link
        if(href.startsWith("http") || href.startsWith("https") ){
            return this;
        }else{
            int overlapLength = 0;
            int maxLength = Math.min(baseUrl.length(), href.length());
            //检查两个字符串是否具有重叠的部分
            for(int i=0; i<maxLength; i++){
                if(baseUrl.endsWith(href.substring(0, i))){
                    overlapLength = i;
                }
            }
            //如果没有重叠的部分
            if(overlapLength == 0){
                href = baseUrl + href;
            }else{
                // 如果有重叠的部分
                href = baseUrl + href.substring(overlapLength);
            }
            this.setHref(href);
        }
        return this;
    }
}
