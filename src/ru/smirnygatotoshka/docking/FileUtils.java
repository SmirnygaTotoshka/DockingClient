package ru.smirnygatotoshka.docking;

import org.apache.hadoop.fs.*;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;

/**
 * @author SmirnygaTotoshka
 */
public final class FileUtils {
	
	/*@Deprecated
	public static BufferedReader readFile(String pathToFile, FileSystem sys) throws IllegalArgumentException, IOException
	{
		BufferedReader reader;
		FSDataInputStream s = sys.open(new Path(pathToFile));
		reader = new BufferedReader(new InputStreamReader(s));
		return reader;
	}*/

    public static ArrayList<String> readFile(String pathToFile, FileSystem sys) throws IOException {
        FSDataInputStream reader = sys.open(new Path(pathToFile));
        ArrayList<String> lines = new ArrayList<>();
        String line = "";
        char t;
        try{
            while (true){
                t = reader.readChar();
                line += Character.toString(t);
                if (t == '\n'){
                    lines.add(line);
                    line = "";
                }
            }
        }
        catch (EOFException e){
            reader.close();
        }
        return lines;
    }

	/*@Deprecated
	public static BufferedWriter writeFile(String pathToFile, FileSystem sys) throws IllegalArgumentException, IOException
	{
		BufferedWriter writer;
		FSDataOutputStream s = sys.create(new Path(pathToFile));
		writer = new BufferedWriter(new OutputStreamWriter(s));

		return writer;
	}*/

    public static void writeFile(ArrayList<String> lines, String pathToFile, FileSystem sys) throws IOException {
        FSDataOutputStream writer = sys.create(new Path(pathToFile));
        for (String line : lines) {
            writer.writeChars(line);
        }
        writer.close();
    }

    public static void copy(FileSystem srcFS, String src, FileSystem dstFS, String dst) throws IOException {
        ArrayList<String> lines = readFile(src, srcFS);
        writeFile(lines, dst, dstFS);
        FileUtil.copy(srcFS,new Path(src),dstFS,new Path(dst),false,true,srcFS.getConf());
    }

    public static void copy(String src, String dst, FileSystem sys) throws IllegalArgumentException, IOException {
        copy(sys, src, sys, dst);
    }

	/*@Deprecated
	public static void copy(String src,String dst,FileSystem sys) throws IllegalArgumentException, IOException	{
		BufferedReader reader = readFile(src,sys);
		BufferedWriter writer = writeFile(dst,sys);
		String line;
		while((line = reader.readLine()) != null)
			writer.write(line + "\n");
		reader.close();
		writer.close();
	}*/

    public static void deleteFolder(Path path, FileSystem sys) throws IOException, TaskException {
            FileStatus[] fileStatuses = sys.listStatus(path);
            for (FileStatus status : fileStatuses) {
                if (!sys.delete(status.getPath(), true))
                    throw new TaskException("Cannot delete file " + status.getPath().toString());
            }
        //if (!sys.delete(path,true)) throw new TaskException("Cannot delete folder " + path);
    }

    public static boolean exist(Path path, FileSystem sys) throws IOException {
		return sys.exists(path);
	}
}
