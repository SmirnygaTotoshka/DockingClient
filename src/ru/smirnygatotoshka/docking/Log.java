package ru.smirnygatotoshka.docking;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

public class Log {
    private BufferedWriter writer;

    public Log(String path) throws IOException {
        this.writer = new BufferedWriter(new FileWriter(path));
    }

    public void write(String line) {
        try {
            writer.write(line);
        } catch (IOException e) {
            e.printStackTrace();
        }
        catch (NullPointerException e){
            try {
                writer.write("null");
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    private void writeDelim() {
        try {
            writer.write("------------------------------------------------------------------------------------\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeRecord(ArrayList<String> msg){
        writeDelim();
        for (String s: msg)
            write(s);
        writeDelim();
    }
    public void close() throws IOException {
        writer.close();
    }
}
