public class Record {//implements Comparable<Record> {


    public void setKey(int key) {
        this.key = key;
    }

    private int key;
    private char[] charset;
    private boolean isDeleted;

    public int getOffsetPointer() {
        return offsetPointer;
    }

    public void setOffsetPointer(int offsetPointer) {
        this.offsetPointer = offsetPointer;
    }

    private int offsetPointer;

    Record(char[] charset){
        this.charset = charset;
        this.key = -1;
        this.offsetPointer = -1;
        this.isDeleted = false;
    }
    Record(int key, char[] charset, int offsetPointer){
        this.charset = charset;
        this.key = key;
        this.offsetPointer = offsetPointer;
        this.isDeleted = false;
    }
    Record(int key, char[] charset, boolean isDeleted, int offsetPointer){
        this.charset = charset;
        this.key = key;
        this.offsetPointer = offsetPointer;
        this.isDeleted = isDeleted;
    }

    public void delete(){this.isDeleted = true;}

    public char[] getCharset(){
        return this.charset;
    }

    public int getKey() {
        return key;
    }

    public boolean isDeleted() {
        return isDeleted;
    }

//    @Override
//    public int compareTo(Record record) {
//        if(record == null) return -1;
//        int max1 = 0, max2 = 0;
//        for(char c1 : charset){
//            for(char c2 : record.getCharset()){
//                if(c1 != c2){
//                    if((int)c1 > max1 )max1 = (int)c1;
//                    if((int)c2 > max2 )max2 = (int)c2;
//                }
//            }
//        }
//        if(max1 > max2 ) return -1;
//        else if (max1 == max2) return 0;
//        else return 1;
//    }
}