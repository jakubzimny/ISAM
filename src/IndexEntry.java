public class IndexEntry {
    public int getKey() {
        return key;
    }

    public void setKey(int key) {
        this.key = key;
    }

    public int getPageNo() {
        return pageNo;
    }

    public void setPageNo(int pageNo) {
        this.pageNo = pageNo;
    }

    private int key;
    private int pageNo;

    public IndexEntry(int key, int page){
        this.key = key;
        this.pageNo = page;
    }

}
