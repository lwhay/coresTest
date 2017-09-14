package col;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;

/**
 * This class tests that blocks can be larger than 2GB
 */
public class TestLargeBlock {
    /**
     * {
     * GenericTestUtils.setLogLevel(DataNode.LOG, Level.ALL);
     * GenericTestUtils.setLogLevel(LeaseManager.LOG, Level.ALL);
     * GenericTestUtils.setLogLevel(FSNamesystem.LOG, Level.ALL);
     * GenericTestUtils.setLogLevel(DFSClient.LOG, Level.ALL);
     * GenericTestUtils.setLogLevel(TestLargeBlock.LOG, Level.ALL);
     * }
     */
    private static final Log LOG = LogFactory.getLog(TestLargeBlock.class);

    // should we verify the data read back from the file? (slow)
    static final boolean verifyData = true;
    static final byte[] pattern = { 'D', 'E', 'A', 'D', 'B', 'E', 'E', 'F' };
    static final int writeSize = 64 * 1024 * 1024;

    // creates a file 
    static FSDataOutputStream createFile(FileSystem fileSys, Path name, int repl, final long blockSize)
            throws IOException {
        FSDataOutputStream stm = fileSys.create(name, true,
                fileSys.getConf().getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096), (short) repl,
                blockSize);
        LOG.info("createFile: Created " + name + " with " + repl + " replica.");
        return stm;
    }

    /**
     * Writes pattern to file
     * 
     * @param stm
     *            FSDataOutputStream to write the file
     * @param fileSize
     *            size of the file to be written
     * @throws IOException
     *             in case of errors
     */
    static void writeFile(FSDataOutputStream stm, final long fileSize) throws IOException {
        // write in chunks of 64 MB
        final int writeSize = pattern.length * 8 * 1024 * 1024;

        if (writeSize > Integer.MAX_VALUE) {
            throw new IOException("A single write is too large " + writeSize);
        }

        long bytesToWrite = fileSize;
        byte[] b = new byte[writeSize];

        // initialize buffer
        for (int j = 0; j < writeSize; j++) {
            b[j] = pattern[j % pattern.length];
        }

        while (bytesToWrite > 0) {
            // how many bytes we are writing in this iteration
            int thiswrite = (int) Math.min(writeSize, bytesToWrite);

            stm.write(b, 0, thiswrite);
            bytesToWrite -= thiswrite;
        }
    }

    /**
     * Reads from file and makes sure that it matches the pattern
     * 
     * @param fs
     *            a reference to FileSystem
     * @param name
     *            Path of a file
     * @param fileSize
     *            size of the file
     * @throws IOException
     *             in case of errors
     */
    static void checkFullFile(FileSystem fs, Path name, final long fileSize) throws IOException {
        // read in chunks of 128 MB
        final int readSize = pattern.length * 16 * 1024 * 1024;

        if (readSize > Integer.MAX_VALUE) {
            throw new IOException("A single read is too large " + readSize);
        }

        byte[] b = new byte[readSize];
        long bytesToRead = fileSize;

        byte[] compb = new byte[readSize]; // buffer with correct data for comparison

        if (verifyData) {
            // initialize compare buffer
            for (int j = 0; j < readSize; j++) {
                compb[j] = pattern[j % pattern.length];
            }
        }

        FSDataInputStream stm = fs.open(name);

        while (bytesToRead > 0) {
            // how many bytes we are reading in this iteration
            int thisread = (int) Math.min(readSize, bytesToRead);

            stm.readFully(b, 0, thisread);

            if (verifyData) {
                // verify data read
                if (thisread == readSize) {
                    //          assertTrue("file is corrupted at or after byte " +
                    //              (fileSize - bytesToRead), Arrays.equals(b, compb));
                } else {
                    // b was only partially filled by last read
                    for (int k = 0; k < thisread; k++) {
                        //            assertTrue("file is corrupted at or after byte " +
                        //                (fileSize - bytesToRead), b[k] == compb[k]);
                    }
                }
            }
            LOG.debug("Before update: to read: " + bytesToRead + "; read already: " + thisread);
            bytesToRead -= thisread;
            LOG.debug("After  update: to read: " + bytesToRead + "; read already: " + thisread);
        }
        stm.close();
    }

    /**
     * Test for block size of 2GB + 512B. This test can take a rather long time to
     * complete on Windows (reading the file back can be slow) so we use a larger
     * timeout here.
     * 
     * @throws IOException
     *             in case of errors
     */
    //  @Test (timeout = 900000)
    public void testLargeBlockSize() throws IOException {
        final long blockSize = 2L * 1024L * 1024L * 1024L + 512L; // 2GB + 512B
        runTest(blockSize);
    }

    /**
     * Test that we can write to and read from large blocks
     * 
     * @param blockSize
     *            size of the block
     * @throws IOException
     *             in case of errors
     */
    public void runTest(final long blockSize) throws IOException {

        // write a file that is slightly larger than 1 block
        final long fileSize = blockSize + 1L;

        Configuration conf = new Configuration();
        MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
        FileSystem fs = cluster.getFileSystem();
        try {

            // create a new file in test data directory
            Path file1 = new Path("/tmp/TestLargeBlock", blockSize + ".dat");
            FSDataOutputStream stm = createFile(fs, file1, 1, blockSize);
            LOG.info("File " + file1 + " created with file size " + fileSize + " blocksize " + blockSize);

            // verify that file exists in FS namespace
            //      assertTrue(file1 + " should be a file", 
            //                  fs.getFileStatus(file1).isFile());

            // write to file
            writeFile(stm, fileSize);
            LOG.info("File " + file1 + " written to.");

            // close file
            stm.close();
            LOG.info("File " + file1 + " closed.");

            // Make sure a client can read it
            checkFullFile(fs, file1, fileSize);

            // verify that file size has changed
            long len = fs.getFileStatus(file1).getLen();
            //      assertTrue(file1 + " should be of size " +  fileSize +
            //                 " but found to be of size " + len, 
            //                  len == fileSize);
        } finally {
            cluster.shutdown();
        }
    }

    static void writeFile(FSDataOutputStream stm, final File file) throws IOException {
        long bytesToWrite = file.length();
        byte[] b = new byte[writeSize];
        FileInputStream input = new FileInputStream(file);

        while (bytesToWrite > 0) {
            // how many bytes we are writing in this iteration
            int thiswrite = (int) Math.min(writeSize, bytesToWrite);
            input.read(b, 0, thiswrite);
            stm.write(b, 0, thiswrite);
            bytesToWrite -= thiswrite;
        }
        input.close();
    }

    static void writeFile(File file, String path, FileSystem fs, long blockSize) throws IOException {
        if (file.isDirectory()) {
            File[] inputs = file.listFiles();
            path += "/" + file.getName();
            for (File input : inputs) {
                writeFile(input, path, fs, blockSize);
            }
        } else {
            fs.mkdirs(new Path(path));
            System.out.println(file.getAbsolutePath() + "----------------------" + path);
            Path out = new Path(path + "/" + file.getName());
            FSDataOutputStream output = createFile(fs, out, 1, blockSize);
            writeFile(output, file);
            output.close();
        }
    }

    public static void main(String[] args) throws IOException {
        final long blockSize = Long.parseLong(args[0]) * 1024 * 1024;
        Configuration conf = new Configuration();
        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
        FileSystem fs = FileSystem.get(conf);
        File file = new File(args[1]);
        String path = args[2];
        writeFile(file, path, fs, blockSize);
    }
}