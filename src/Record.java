public class Record implements Comparable<Record> {

    private int key;
    private char[] charset;
    private boolean isDeleted;
    private int offsetPointer;

    Record(char[] charset){
        this.charset = charset;
        isDeleted = false;
    }

    public char[] getCharset(){
        return this.charset;
    }

    @Override
    public int compareTo(Record record) {
        if(record == null) return -1;
        int max1 = 0, max2 = 0;
        for(char c1 : charset){
            for(char c2 : record.getCharset()){
                if(c1 != c2){
                    if((int)c1 > max1 )max1 = (int)c1;
                    if((int)c2 > max2 )max2 = (int)c2;
                }
            }
        }
        if(max1 > max2 ) return -1;
        else if (max1 == max2) return 0;
        else return 1;
    }
}