package ru.smirnygatotoshka.docking;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.logging.Level;

import org.apache.hadoop.fs.*;
import ru.smirnygatotoshka.exception.TaskException;

/**
 * @author SmirnygaTotoshka
 *
 */
public final class FileUtils {
	
	
	public static BufferedReader readFile(String pathToFile, FileSystem sys) throws IllegalArgumentException, IOException
	{
		BufferedReader reader;
		FSDataInputStream s = sys.open(new Path(pathToFile));
		reader = new BufferedReader(new InputStreamReader(s));
		return reader;
	}
	
	public static BufferedWriter writeFile(String pathToFile, FileSystem sys) throws IllegalArgumentException, IOException
	{
		BufferedWriter writer;
		FSDataOutputStream s = sys.create(new Path(pathToFile));
		writer = new BufferedWriter(new OutputStreamWriter(s));

		return writer;
	}
	public static void copy(FileSystem srcFS,String src,FileSystem dstFS,String dst) throws IOException {
		FileUtil.copy(srcFS,new Path(src),dstFS,new Path(dst),false,true,srcFS.getConf());
	}

	public static void copy(String src,String dst,FileSystem sys) throws IllegalArgumentException, IOException
	{
		BufferedReader reader = readFile(src,sys);
		BufferedWriter writer = writeFile(dst,sys);
		String line;
		while((line = reader.readLine()) != null)
			writer.write(line + "\n");
		reader.close();
		writer.close();
	}

	public static void deleteFolder(Path path,FileSystem sys) throws IOException,TaskException
	{
		FileStatus[] fileStatuses = sys.listStatus(path);
		for (FileStatus status : fileStatuses)
		{
			String name = status.getPath().getName();
			if (!name.contains(".dlg"))
				if (!sys.delete(status.getPath(),true)) throw new TaskException("Cannot delete file " + status.getPath().toString());
		}
		//if (!sys.delete(path,true)) throw new TaskException("Cannot delete folder " + path);
	}
	public static boolean exist(Path path,FileSystem sys) throws IOException {
		return sys.exists(path);
	}
}
