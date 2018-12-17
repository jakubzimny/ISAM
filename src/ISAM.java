import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;


public class ISAM {

    private final int BLOCKING_FACTOR = 10;
    private final int RECORD_SIZE = 60 + 4 + 4 + 1;
    private final double ALPHA = 0.6;
    private final double PRIMARY_TO_OVERFLOW_RATIO = 0.4;

    public ArrayList<IndexEntry> getIndex() {
        return index;
    }

    private ArrayList<IndexEntry> index;

    private String dataFile = "data.dat";
    private String indexFile = "index.dat";
    private String tempDataFile = "tempData.dat";

     ISAM() {
        index = new ArrayList<>();
        File tmpFile = new File(indexFile);
        if (tmpFile.exists()) loadIndex();
    }

    public void generateRandom(int n) {

    }

    public void saveIndex() {
        File f = new File(indexFile);
        if(index.size()!=0) {
            try (FileOutputStream fos = new FileOutputStream(f);
                 DataOutputStream dos = new DataOutputStream(fos)) {
                for (IndexEntry ie : index) {
                    dos.writeInt(ie.getKey());
                    dos.writeInt(ie.getPageNo());
                }
            } catch (IOException ex) {
                System.err.println(ex.getMessage());
            }
        }
    }

    private void loadIndex() {
        File f = new File(indexFile);
        try (FileInputStream fis = new FileInputStream(f);
             DataInputStream dis = new DataInputStream(fis)) {
            while (true) {
                int key = dis.readInt();
                int pageNo = dis.readInt();
                index.add(new IndexEntry(key, pageNo));
            }
        } catch (EOFException eof) {
            //EOF
        } catch (IOException ex) {
            System.err.println(ex.getMessage());
        }
    }

    public int getPagesNumber() {
        File f = new File(dataFile);
        return (int)(f.length() / RECORD_SIZE) / BLOCKING_FACTOR;
    }

    private void assignKey(Record r) {
        r.setKey(new String(r.getCharset()).hashCode());
    }

    public void reorganize() {
        int pageCounter=1;
        ArrayList<IndexEntry> newIndex = new ArrayList<>();
        ArrayList<Record> newPageBuffer = new ArrayList<>();
        ArrayList<Record> overflowPage = new ArrayList<>();
        for(int i= 1; i<= index.size(); i++){
            ArrayList<Record> pageBuffer = getPage(i);
            if(i==1) newIndex.add(new IndexEntry(pageBuffer.get(0).getKey(),pageCounter));
            if(newIndex.get(pageCounter-1).getKey() == -1)
                newIndex.get(pageCounter-1).setKey(pageBuffer.get(0).getKey());
            int lastOAPageNo = -1;
            for (Record r : pageBuffer){
                int offset = r.getOffsetPointer();
                r.setOffsetPointer(-1);
                newPageBuffer.add(r);
                while(offset != -1){
                    if(newPageBuffer.size() >= (int)Math.ceil(BLOCKING_FACTOR* ALPHA)){
                        savePage(newPageBuffer, pageCounter, tempDataFile);
                        pageCounter++;
                        newIndex.add(new IndexEntry(-1,pageCounter));
                        newPageBuffer.clear();
                    }
                    if(calculatePageNumber(offset) != lastOAPageNo)
                        overflowPage = getPage(calculatePageNumber(offset));
                    lastOAPageNo = calculatePageNumber(offset);
                    Record record = overflowPage.get(calculatePageOffset(offset));
                    offset = record.getOffsetPointer();
                    record.setOffsetPointer(-1);
                    if(newIndex.get(pageCounter-1).getKey() == -1)
                        newIndex.get(pageCounter-1).setKey(record.getKey());
                    newPageBuffer.add(record);
                }
                if(newPageBuffer.size() >= (int)Math.ceil(BLOCKING_FACTOR* ALPHA)){
                    savePage(newPageBuffer, pageCounter, tempDataFile);
                    pageCounter++;
                    newIndex.add(new IndexEntry(-1,pageCounter));
                    newPageBuffer.clear();
                }
            }
        }
        if(newPageBuffer.size()!=0)
            savePage(newPageBuffer, pageCounter, tempDataFile);
        if (newIndex.get(newIndex.size()-1).getKey() == -1)
            newIndex.remove(newIndex.size()-1);
        index = newIndex;
        File fData = new File(dataFile);
        File fIndex = new File(indexFile);
        File newData = new File(tempDataFile);
        fData.delete();
        fIndex.delete();
        saveIndex();
        newData.renameTo(new File(dataFile));
        for (int i = 1; i <= (int)Math.ceil(index.size()*PRIMARY_TO_OVERFLOW_RATIO); i++){
            savePage(new ArrayList<>(), index.size()+i, dataFile);
        }
    }

    private int saveToOverflow(Record r) { // save to overflow and return offset pointer
        int pageNo = -1, ret = -1;
        ArrayList<Record> page = new ArrayList<>();
        for (int i = index.size() + 1; i <= getPagesNumber(); i++) {
            page = getPage(i);
            if (page.size() < BLOCKING_FACTOR) {
                pageNo = i;
                break;
            }
        }
        if (pageNo != -1) {
            page.add(r);
            ret = (page.size() - 1) * RECORD_SIZE + pageNo * RECORD_SIZE * BLOCKING_FACTOR;
            savePage(page, pageNo, dataFile);
        } else System.err.println("Cannot save page to overflow area - there is no space left.");
        return ret;
    }

    public void insert(Record r) {
        //TODO if overflow => reorganize
        assignKey(r);
        ArrayList<Record> pageBuffer = new ArrayList<>();
        if (index.size() == 0) {  //index empty
            pageBuffer.add(r);
            index.add(new IndexEntry(r.getKey(), 1));
            savePage(pageBuffer, 1, dataFile);
            savePage(new ArrayList<>(), 2, dataFile); //1 overflow page at start
            return;
        }
        int pageNo = indexLookup(r.getKey());
        pageBuffer = getPage(pageNo);
        for(Record rec: pageBuffer){ // Test if record is unique
            if(rec.getKey() == r.getKey()){
                System.err.println("This record already exists.");
                return;
            }
        }
        if (pageNo == 1 && r.getKey() < pageBuffer.get(0).getKey()) { //new smallest record
            Record temp = pageBuffer.get(0);
            pageBuffer.set(0, r);
            int offset = saveToOverflow(temp);
            pageBuffer.get(0).setOffsetPointer(offset);
            savePage(pageBuffer, pageNo, dataFile);
            index.get(0).setKey(r.getKey());
            return;
        }
        if (pageBuffer.size() >= BLOCKING_FACTOR) { //page full so write in overflow area
            int offsetPointer = saveToOverflow(r);
            updateOffset(pageBuffer, r, offsetPointer, pageBuffer.size(), pageNo);
        }
        else { // empty space in page
            int placement = pageBuffer.size();
            for (int i = 0; i < pageBuffer.size(); i++) {
                if (r.getKey() < pageBuffer.get(i).getKey()) {
                    placement = i;
                    break;
                }
            }
            if (placement == pageBuffer.size()) { // if can be placed at the end then insert in primary area
                pageBuffer.add(r);
                savePage(pageBuffer, pageNo, dataFile);
            } else { // else overflow area
                int offsetPointer = saveToOverflow(r);
                updateOffset(pageBuffer, r, offsetPointer, placement, pageNo);
            }
        }
    }

    private void updateOffset(ArrayList<Record> pageBuffer, Record r, int offsetPointer, int placement, int pageNo) {
        int oldOffset = pageBuffer.get(placement - 1).getOffsetPointer();
        if (oldOffset != -1) {
            ArrayList<Record> temp = getPage(calculatePageNumber(oldOffset));
            int lastPageNo = calculatePageNumber(oldOffset);
            boolean done = false;
            Record overflowRecord = temp.get(calculatePageOffset(oldOffset));
            while (overflowRecord.getKey() < r.getKey()) {
                if (overflowRecord.getOffsetPointer() == -1) {
                    overflowRecord.setOffsetPointer(offsetPointer);
                    temp.set(calculatePageOffset(oldOffset), overflowRecord);
                    savePage(temp, calculatePageNumber(oldOffset), dataFile);
                    done = true;
                    break;
                }
                oldOffset = overflowRecord.getOffsetPointer();
                if (calculatePageNumber(oldOffset) != lastPageNo) {
                    temp = getPage(calculatePageNumber(oldOffset));
                    lastPageNo = calculatePageNumber(oldOffset);
                }
                overflowRecord = temp.get(calculatePageOffset(oldOffset));
            }
            if (!done) {
                pageBuffer.get(placement - 1).setOffsetPointer(offsetPointer);
                r.setOffsetPointer(oldOffset);
                ArrayList<Record> temp2 = getPage(calculatePageNumber(offsetPointer));
                temp2.set(calculatePageOffset(offsetPointer), r);
                savePage(temp2, calculatePageNumber(offsetPointer), dataFile);
            }
        } else pageBuffer.get(placement - 1).setOffsetPointer(offsetPointer);
        savePage(pageBuffer, pageNo, dataFile);
    }

    private int calculatePageNumber(int offsetPointer) {
        return offsetPointer / (BLOCKING_FACTOR * RECORD_SIZE);
    }

    private int calculatePageOffset(int offsetPointer) {
        return (offsetPointer % (BLOCKING_FACTOR * RECORD_SIZE)) / RECORD_SIZE;
    }

    private void savePage(ArrayList<Record> page, int pageNo, String fileName) {
        File f = new File(fileName);
        try {
            f.createNewFile();
            RandomAccessFile raf = new RandomAccessFile(f, "rw");
            raf.seek((pageNo - 1) * BLOCKING_FACTOR * RECORD_SIZE);
            ByteBuffer b = ByteBuffer.allocate(RECORD_SIZE * BLOCKING_FACTOR);
            for (Record r : page) {
                b.putInt(r.getKey());
                for (char c : r.getCharset()) b.putChar(c);
                for (int i = 0; i < 30 - r.getCharset().length; i++) {
                    b.put((byte) 0x00);
                    b.put((byte) 0x00);
                }
                b.put((byte) (r.isDeleted() ? 0x01 : 0x00));
                b.putInt(r.getOffsetPointer());
            }
            raf.write(b.array());
            raf.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public ArrayList<Record> getPage(int pageNo) {
        File f = new File(dataFile);
        ArrayList<Record> page = new ArrayList<>();
        try {
            f.createNewFile();
            RandomAccessFile raf = new RandomAccessFile(f, "rw");
            raf.seek((pageNo - 1) * BLOCKING_FACTOR * RECORD_SIZE);
            byte[] bytes = new byte[BLOCKING_FACTOR * RECORD_SIZE];
            int bytesRead = raf.read(bytes);
            for (int i = 0; i < bytesRead; i += RECORD_SIZE) {
                int key = (bytes[i] << 24) | ((bytes[i + 1] & 0xFF) << 16) | ((bytes[i + 2] & 0xFF) << 8) | (bytes[i + 3] & 0xFF);
                if (key == 0) continue;
                StringBuilder sb = new StringBuilder("");
                for (int j = 0; j < 60; j++) {
                    if (bytes[i + 4 + j] == 0) continue;
                    sb.append((char) bytes[i + 4 + j]);
                }
                boolean isDeleted = bytes[64] == 1;
                int offsetPointer = (bytes[i + 65] << 24) | ((bytes[i + 66] & 0xFF) << 16) | ((bytes[i + 67] & 0xFF) << 8) | (bytes[i + 68] & 0xFF);
                page.add(new Record(key, sb.toString().toCharArray(), isDeleted, offsetPointer));
            }
            raf.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return page;
    }

    private int indexLookup(int key) {
        int lastKey = -1;
        for (IndexEntry i : index) {
            if (lastKey == -1) {
                lastKey = i.getKey();
                continue;
            }
            if (i.getKey() > key && lastKey <= key) return i.getPageNo() - 1;
            lastKey = i.getKey();
        }
        return index.size();
    }
}
