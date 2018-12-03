import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

public class ISAM {

    private final int BLOCKING_FACTOR = 10;
    private final int RECORD_SIZE = 60+4+4+1;
    private ArrayList<IndexEntry> index;

    private String dataFileName = "data.dat";
    private String indexFileName = "index.dat";

    public ISAM(){
        index = null;
        ArrayList<Record> page = new ArrayList<>();
        for(int i=0; i<BLOCKING_FACTOR; i++){
            page.add(new Record(i,"asada".toCharArray(), false,-1));
        }
        appendNewPage(page,0);
    }

    public void generateRandom(int n){

    }

    public void insert(Record r){
        //if overflow => reorganize
        int key=0; //key is sum of all ASCII codes in records times ASCII code of first character
        char[] charset = r.getCharset();
        for (char c : charset){
            key+=c;
        }
        key *= charset[0];
        //int pageNo = indexLookup(key);
        //ArrayList<Record> page = getPage(pageNo);

    }

    public void appendNewPage(ArrayList<Record> page, int pageNo){
        File f = new File(dataFileName);
        try {
            f.createNewFile();
            RandomAccessFile raf = new RandomAccessFile(f, "rw");
            raf.seek(pageNo * BLOCKING_FACTOR * RECORD_SIZE);
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
            raf.seek(pageNo * BLOCKING_FACTOR * RECORD_SIZE);
            byte[] bytes = new byte[BLOCKING_FACTOR * RECORD_SIZE];
            int bytesRead = raf.read(bytes);
            for(int i=0; i<bytesRead; i+=RECORD_SIZE){
                int key = (bytes[i] << 24) | ((bytes[i+1]&0xFF) << 16) | ((bytes[i+2]&0xFF) << 8) | (bytes[i+3] & 0xFF);
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

    public int indexLookup(int key){
        int lastKey = -1;
        for (IndexEntry i : index){
            if(lastKey == -1){
                lastKey = i.getKey();
                continue;
            }
            if(i.getKey() >= key && lastKey <= key) return i.getPageNo()-1;
            lastKey = i.getKey();
        }
        return index.size()-1;
    }
}
