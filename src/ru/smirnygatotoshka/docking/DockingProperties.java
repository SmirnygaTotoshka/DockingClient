package ru.smirnygatotoshka.docking;

/**
 * Содержит описание подготовительных этапов для докинга
 *
 * @author SmirnygaTotoshka
 */
public class DockingProperties {
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
        this.pathToFiles = "/";
        this.receptor = "receptor.pdbqt";
        this.receptorFlexiblePart = "receptorFlexiblePart.pqbqt";
        this.ligand = "ligand.pdbqt";
        this.gpfName = "gpfName.gpf";
        this.gpfParameters = "";
        this.dpfName = "dpfName.gpf";
        this.dpfParameters = "";
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
        String id = "";
        try{
            if (receptorFlexiblePart.contentEquals(""))
                id =  receptor.substring(0, receptor.indexOf('.')) + "_" +
                        ligand.substring(0, ligand.indexOf('.')) + "_None_" +
                        gpfName.substring(0, gpfName.indexOf('.'));
            else
                id = receptor.substring(0, receptor.indexOf('.')) + "_" +
                        ligand.substring(0, ligand.indexOf('.')) + "_" +
                        receptorFlexiblePart.substring(0, receptorFlexiblePart.indexOf('.')) + "_" +
                        gpfName.substring(0, gpfName.indexOf('.'));
        }
        catch (StringIndexOutOfBoundsException e) {
            id = receptor + "_" + ligand + "_" + receptorFlexiblePart + "_" + gpfName;
        }
        return id;
    }

    /**
     * @return путь к файлу рецептора в hdfs
     */
    public String getReceptorPath() {
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
