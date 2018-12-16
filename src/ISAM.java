import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Random;

public class ISAM {

    private final int BLOCKING_FACTOR = 10;
    private final int RECORD_SIZE = 60+4+4+1;

    public ArrayList<IndexEntry> getIndex() {
        return index;
    }

    private ArrayList<IndexEntry> index;

    private String dataFileName = "data.dat";
    private String indexFileName = "index.dat";

    public String getDataFileName() {
        return dataFileName;
    }

    public ISAM(){
        index = new ArrayList<>();
        File tmpFile = new File(indexFileName);
        if (tmpFile.exists())loadIndex();
    }

    public void generateRandom(int n){

    }

    private void loadIndex(){
        //TODO load index file from disk
    }

    public long getPagesNumber(){
        File f = new File(dataFileName);
        return (f.length()/RECORD_SIZE)/BLOCKING_FACTOR;
    }

    private void assignKey(Record r){
//        int key=0; //key is sum of all ASCII codes in record - offset
//        char[] charset = r.getCharset();
//        for (char c : charset){
//            key+=c;
//        }
//        key -= 32;
//        r.setKey(key);
        r.setKey(new String(r.getCharset()).hashCode());
    }

    public void reorganize(){
        //TODO reorganize handling
    }

    private int saveToOverflow(Record r){ // save to overflow and return offset pointer
        int pageNo= -1, ret = -1;
        ArrayList<Record> page = new ArrayList<>();
        for(int i =index.size()+1; i<=getPagesNumber(); i++) {
           page = getPage(i);
            if (page.size() < BLOCKING_FACTOR) {
                pageNo = i;
                break;
            }
        }
        if(pageNo!=-1) {
            page.add(r);
            ret = page.size()-1 + pageNo*RECORD_SIZE;
            savePage(page, pageNo);
        }
        else System.err.println("Cannot save page to Overflow Area - there is no space left.");
        return ret;
    }

    public void insert(Record r){
        //TODO if overflow => reorganize
        assignKey(r);
        ArrayList<Record> pageBuffer = new ArrayList<>();
        if(index.size()==0){  //index empty
            pageBuffer.add(r);
            index.add(new IndexEntry(r.getKey(), 1));
            savePage(pageBuffer,1);
            savePage(new ArrayList<>(), 2); //1 overflow page at start
            return;
        }

        int pageNo = indexLookup(r.getKey());
        pageBuffer = getPage(pageNo);
        if(pageNo == 1){
            //TODO new smallest record handling
        }
        if(pageBuffer.size() >= BLOCKING_FACTOR){ //page full so write in overflow area
            if(pageBuffer.get(pageBuffer.size()-1).getOffsetPointer() != -1) { //12 -> 13 case
                r.setOffsetPointer(pageBuffer.get(pageBuffer.size()-1).getOffsetPointer()); //TODO refactoring (DRY violation)
            }
            int offsetPointer = saveToOverflow(r);
            pageBuffer.get(pageBuffer.size()-1).setOffsetPointer(offsetPointer);
            savePage(pageBuffer,pageNo);

        }
        else{ // empty space in page
            int placement=pageBuffer.size();
            for(int i=0; i<pageBuffer.size(); i++ ){
                if(r.getKey() < pageBuffer.get(i).getKey()){
                 placement = i;
                 break;
                }
            }
            if(placement == pageBuffer.size()){ // if can be placed at the end then insert in primary area
                pageBuffer.add(r);
                savePage(pageBuffer, pageNo);
            }
            else{ // else overflow area
                if(pageBuffer.get(pageBuffer.size()-1).getOffsetPointer() != -1) { //12 -> 13 case
                    r.setOffsetPointer(pageBuffer.get(pageBuffer.size()-1).getOffsetPointer()); //TODO refactoring (DRY violation)
                }
                int offsetPointer = saveToOverflow(r);
                pageBuffer.get(placement-1).setOffsetPointer(offsetPointer);
                savePage(pageBuffer,pageNo);
            }
        }
    }

    private void savePage(ArrayList<Record> page, int pageNo){
        File f = new File(dataFileName);
        try {
            f.createNewFile();
            RandomAccessFile raf = new RandomAccessFile(f, "rw");
            raf.seek((pageNo-1) * BLOCKING_FACTOR * RECORD_SIZE );
            ByteBuffer b = ByteBuffer.allocate(RECORD_SIZE*BLOCKING_FACTOR);
            for(Record r: page){
                b.putInt(r.getKey());
                for(char c: r.getCharset())b.putChar(c);
                for(int i=0; i < 30 - r.getCharset().length; i++){
                    b.put((byte)0x00);
                    b.put((byte)0x00);
                }
                b.put((byte)(r.isDeleted()? 0x01 : 0x00));
                b.putInt(r.getOffsetPointer());
            }
            raf.write(b.array());
           // byte[] freeBytes = new byte[RECORD_SIZE];
          //  for (int j=0; j<RECORD_SIZE; j++)freeBytes[j]=(byte)0xFF;
          //  for(int i=page.size(); i < BLOCKING_FACTOR; i++){
          //      raf.write(freeBytes);
          //  }
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    public ArrayList<Record> getPage(int pageNo){
        File f = new File(dataFileName);
        ArrayList<Record> page = new ArrayList<>();
        try {
            f.createNewFile();
            RandomAccessFile raf = new RandomAccessFile(f, "rw");
            raf.seek((pageNo-1) * BLOCKING_FACTOR * RECORD_SIZE);
            byte[] bytes = new byte[BLOCKING_FACTOR * RECORD_SIZE];
            int bytesRead = raf.read(bytes);
            for(int i=0; i<bytesRead; i+=RECORD_SIZE){
                int key = (bytes[i] << 24) | ((bytes[i+1]&0xFF) << 16) | ((bytes[i+2]&0xFF) << 8) | (bytes[i+3] & 0xFF);
                if(key==-1 || key==0)continue;
                StringBuilder sb = new StringBuilder("");
                for(int j=0; j<60; j++){
                    if(bytes[i+4+j] == 0)continue;
                    sb.append((char)bytes[i+4+j]);
                }
                boolean isDeleted = bytes[64] == 1;
                int offsetPointer = (bytes[i+65] << 24) | ((bytes[i+66]&0xFF) << 16) | ((bytes[i+67]&0xFF) << 8) | (bytes[i+68] & 0xFF);
                page.add(new Record(key, sb.toString().toCharArray(), isDeleted, offsetPointer));
            }
        }catch (IOException e) {
            e.printStackTrace();
        }
        return page;
    }

    private int indexLookup(int key){
        int lastKey = -1;
        for (IndexEntry i : index){
           // if(key<=index.get(0).getKey())return 1;
            if(lastKey == -1){
                lastKey = i.getKey();
                continue;
            }
            if(i.getKey() > key && lastKey <= key) return i.getPageNo()-1;
            lastKey = i.getKey();
        }
        return index.size();
    }
}
