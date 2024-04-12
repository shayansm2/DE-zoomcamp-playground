package org.example;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.GZIPInputStream;

public class GithubArchiveUtils {
    public static String getGitHubArchiveFile(int year, int month, int day, int hour) throws IOException, URISyntaxException {
        String fileName = getFileName(year, month, day, hour);
        String filePath = String.format("./files/%s", fileName);
        Path path = Paths.get(filePath);
        if (Files.exists(path)) {
            return filePath;
        }

        String zipFilePath = download(fileName);
        unzip(zipFilePath, filePath);

        return filePath;
    }

    private static String getFileName(int year, int month, int day, int hour) {
        return String.format("%d-%02d-%02d-%d.json", year, month, day, hour);
    }

    private static String download(String fileName) throws IOException, URISyntaxException {
        URL urlObj = new URI(String.format("https://data.gharchive.org/%s.gz", fileName)).toURL();
        ReadableByteChannel channel = Channels.newChannel(urlObj.openStream());
        String zipFilePath = String.format("./files/%s.gz", fileName);

        FileOutputStream fOutStream = new FileOutputStream(zipFilePath);
        fOutStream.getChannel().transferFrom(channel, 0, Long.MAX_VALUE);
        System.out.println("File Successfully Downloaded from the URL and saved as: " + zipFilePath);
        return zipFilePath;
    }

    private static void unzip(String fromPath, String toPath) throws IOException {
        byte[] buffer = new byte[1024];
        GZIPInputStream gzis = new GZIPInputStream(new FileInputStream(fromPath));
        FileOutputStream out = new FileOutputStream(toPath);

        int len;
        while ((len = gzis.read(buffer)) > 0) {
            out.write(buffer, 0, len);
        }

        gzis.close();
        out.close();
        System.out.println("File decompressed successfully!");
    }
}
