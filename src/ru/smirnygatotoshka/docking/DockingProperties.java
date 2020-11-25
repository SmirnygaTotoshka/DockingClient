package ru.smirnygatotoshka.docking;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Содержит описание подготовительных этапов для докинга
 *
 * @author SmirnygaTotoshka
 */
public class DockingProperties implements Writable {
//TODO - tests
    /**
     * Путь к файлам в hdfs
     * */
    private String pathToFiles;

    /**
     * Имя файла рецептора. Должен иметь формат pdbqt
     * */
    private String receptor;

    /**
     * Имя файла гибкой части рецептора. Может быть не указано. Должен иметь формат pdbqt
     * */
    private String receptorFlexiblePart;

    /**
     * Имя файла лиганда. Должен иметь формат pdbqt
     * */
    private String ligand;

    /**
     * Имя файла с параметрами autogrid. Будет сгенерирован. Должен иметь формат gpf
     * */
    private String gpfName;

    /**
     * Строка с опциональными параметрами скрипта prepare_gpf4.py.
     * */
    private String gpfParameters;

    /**
     * Имя файла с параметрами autodock. Будет сгенерирован. Должен иметь формат dpf
     * */
    private String dpfName;

    /**
     * Строка с опциональными параметрами скрипта prepare_dpf42.py.
     * */
    private String dpfParameters;

    public DockingProperties(String pathToFiles, String receptor, String receptorFlexiblePart, String ligand,
                             String gpfName, String gpfParameters, String dpfName, String dpfParameters) {
        this.pathToFiles = pathToFiles;
        this.receptor = receptor;
        this.receptorFlexiblePart = receptorFlexiblePart;
        this.ligand = ligand;
        this.gpfName = gpfName;
        this.gpfParameters = gpfParameters;
        this.dpfName = dpfName;
        this.dpfParameters = dpfParameters;
    }

    DockingProperties() {

    }

    public String getPathToFiles() {
        return pathToFiles;
    }

    public String getReceptor() {
        return receptor;
    }

    public String getReceptorFlexiblePart() {
        return receptorFlexiblePart;
    }

    public String getLigand() {
        return ligand;
    }

    public String getGpfName() {
        return gpfName;
    }

    public String getGpfParameters() {
        return gpfParameters;
    }

    public String getDpfName() {
        return dpfName;
    }

    public String getDpfParameters() {
        return dpfParameters;
    }

    /**@return Возвращает идентификатор задания:рецептор_лиганд_гибкая часть рецептора_имя gpf*/
    public String getId() {
        return receptor.substring(0, receptor.charAt('.')) + "_" +
                ligand.substring(0, ligand.charAt('.')) + "_" +
                receptorFlexiblePart.substring(0, receptorFlexiblePart.charAt('.')) + "_" +
                gpfName.substring(0, gpfName.charAt('.'));
    }

    @Override
    public void readFields(DataInput arg0) throws IOException {
        pathToFiles = arg0.readUTF();
        receptor = arg0.readUTF();
        receptorFlexiblePart = arg0.readUTF();
        ligand = arg0.readUTF();
        gpfName = arg0.readUTF();
        gpfParameters = arg0.readUTF();
        dpfName = arg0.readUTF();
        dpfParameters = arg0.readUTF();
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
        arg0.writeUTF(pathToFiles);
        arg0.writeUTF(receptor);
        arg0.writeUTF(receptorFlexiblePart);
        arg0.writeUTF(ligand);
        arg0.writeUTF(gpfName);
        arg0.writeUTF(gpfParameters);
        arg0.writeUTF(dpfName);
        arg0.writeUTF(dpfParameters);
    }

    /**
     * @return путь к файлу рецептора в hdfs
     */
    public String getReceptorPath()
    {
	return pathToFiles + "/" + receptor;
    }

    /**
     * @return путь к файлу лиганда в hdfs
     */
    public String getLigandPath()
    {
	return  pathToFiles + "/" + ligand;
    }

    /**
     * @return путь к файлу гибкой части рецептора в hdfs
     */
    public String getReceptorFlexiblePartPath() {
        if (receptorFlexiblePart.contentEquals(""))
            return receptorFlexiblePart;
        return pathToFiles + "/" + receptorFlexiblePart;
    }

    @Override
    public String toString() {
        String m = "DockingProperties:" + getId() + ";" + "\n" +
                "Receptor:" + receptor + ";" + "\n" +
                "ReceptorFlexPart:" + receptorFlexiblePart + ";" + "\n" +
                "Ligand:" + ligand + ";" + "\n" +
                "gpfName:" + gpfName + ";" + "\n" +
                "gpfParameters:" + gpfParameters + ";" + "\n" +
                "dpfName:" + dpfName + ";" + "\n" +
                "dpfParameters:" + dpfParameters + ";" + "\n";
        return m;
    }
}
