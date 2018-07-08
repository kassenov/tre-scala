package reader;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.IOUtils;
import scala.collection.concurrent.Map;
import scala.collection.concurrent.TrieMap;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ArchiveReader {

    public static final Logger LOG = Logger.getLogger(ArchiveReader.class.getName());

    public Map<String, String> getArchivedFiles(String tarFile) {

        Map<String, String> nameToJsonStrMap = new TrieMap<String, String>();

        try {
            InputStream fin = Files.newInputStream(Paths.get(tarFile));
            BufferedInputStream in = new BufferedInputStream(fin);
            TarArchiveInputStream tarInputStream = new TarArchiveInputStream(in);

            ArchiveEntry nextEntry;
            while ((nextEntry = tarInputStream.getNextEntry()) != null) {
                String name = nextEntry.getName();
                if (!nextEntry.isDirectory()) {
                    String jsonStr = new String(IOUtils.toByteArray(tarInputStream));
                    nameToJsonStrMap.put(name, jsonStr);
                }
            }

            tarInputStream.close();
        } catch (IOException ioe) {
            LOG.log(Level.SEVERE, "Archive file cannot be extracted: " + tarFile, ioe);
        }

        return nameToJsonStrMap;
    }

}
