package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc td;
    private int fileId = 0;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *          the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        // some code goes here
        try {
            this.fileId = f.getAbsolutePath().hashCode();
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return fileId;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws IllegalArgumentException {
        // some code goes here
        int pageDiffernce = pid.getPageNumber() * BufferPool.getPageSize();
        if (pid.getPageNumber() >= numPages()) {
            throw new IllegalArgumentException();
        } else {
            byte[] data = new byte[BufferPool.getPageSize()];
            try (RandomAccessFile rAccessFile = new RandomAccessFile(file, "r");) {
                rAccessFile.seek(pageDiffernce);
                try {
                    rAccessFile.readFully(data);
                } catch (EOFException e) {
                }
                HeapPage heapPage = new HeapPage((HeapPageId) pid, data);
                return heapPage;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int pageDiffernce = page.getId().getPageNumber() * BufferPool.getPageSize();
        byte[] data = page.getPageData();
        try (RandomAccessFile rAccessFile = new RandomAccessFile(file, "rw");) {
            rAccessFile.seek(pageDiffernce);
            rAccessFile.write(data);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        int pageSize = BufferPool.getPageSize();
        long fileSize = file.length();
        if ((int) (fileSize % (long) pageSize) == 0) {
            return (int) (fileSize / (long) pageSize);
        } else {
            return (int) (fileSize / (long) pageSize) + 1;
        }
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        BufferPool bufferPool = Database.getBufferPool();
        int i = 0;
        for (; i < numPages(); i++) {
            HeapPage p = (HeapPage) bufferPool.getPage(tid, new HeapPageId(getId(), i), null);
            try {
                p.insertTuple(t);
            } catch (DbException e) {
                continue;
            }
            return Arrays.asList(p);
        }
        // 尝试分配一个新页
        HeapPage p = new HeapPage(new HeapPageId(getId(), i), HeapPage.createEmptyPageData());
        p.insertTuple(t);
        writePage(p);
        bufferPool.addPage(p.getId(), p);
        return Arrays.asList(p);
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        BufferPool bufferPool = Database.getBufferPool();
        HeapPage p = (HeapPage) bufferPool.getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        p.deleteTuple(t);
        return Arrays.asList(p);
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {
            private int pagePos = 0;
            private boolean closed = true;
            private Iterator<Tuple> pageIterator = getPageIterator(new HeapPageId(getId(), 0));

            private Iterator<Tuple> getPageIterator(PageId pid) {
                try {
                    HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                    return page.iterator();
                } catch (TransactionAbortedException | DbException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (closed)
                    return false;
                if (pageIterator.hasNext()) {
                    return true;
                }
                while (++pagePos < numPages()) {
                    pageIterator = getPageIterator(new HeapPageId(getId(), pagePos));
                    if (pageIterator != null) {
                        if (pageIterator.hasNext())
                            return true;
                    }
                }
                return false;
            }

            @Override
            public void open() throws DbException, TransactionAbortedException {
                closed = false;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (closed)
                    throw new NoSuchElementException();
                if (!hasNext()) {
                    if (pagePos + 1 >= numPages())
                        throw new NoSuchElementException();
                    pageIterator = getPageIterator(new HeapPageId(getId(), ++pagePos));
                    if (pageIterator == null)
                        throw new NoSuchElementException();
                }
                return pageIterator.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                pagePos = 0;
                pageIterator = getPageIterator(new HeapPageId(getId(), pagePos));
            }

            @Override
            public void close() {
                closed = true;
            }

        };
    }

}
