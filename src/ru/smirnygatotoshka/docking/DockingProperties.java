package ru.smirnygatotoshka.docking;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import ru.smirnygatotoshka.exception.MovingDockingException;

public class DockingProperties implements Writable
{

    private String pathToFiles;

    private String receptor;

    private String ligand;

    private String receptorFlexiblePart;

    private String startGPF;

    private String finishGPF;
    
    private String optionalString;

    private int divisionNumber;

    public DockingProperties(String receptor, String ligand, String receptor_flex_part, String startGPF,
                             String finishGPF, int divisionNumber,String pathToFiles,String optionalString) throws MovingDockingException
    {
        set(receptor,ligand,receptor_flex_part,startGPF,finishGPF,divisionNumber,pathToFiles,
                optionalString);
    }

    public DockingProperties()
    {
        super();
    }

    public void set(DockingProperties properties) throws MovingDockingException
    {
        set(properties.receptor,properties.ligand,properties.receptorFlexiblePart,properties.startGPF,properties.finishGPF,properties.divisionNumber,properties.pathToFiles,
                properties.optionalString);
    }

    public void set(String receptor, String ligand, String receptor_flex_part, String startGPF,
                         String finishGPF, int divisionNumber,String pathToFiles,String optionalString) throws MovingDockingException
    {
        if (!finishGPF.equals(startGPF) & divisionNumber == 1)
            throw new MovingDockingException("Попытка запуска динамического докинга с неправильными настройками.");

        this.receptor = receptor;
        this.ligand = ligand;
        this.receptorFlexiblePart = receptor_flex_part;

        this.startGPF = startGPF;
        this.finishGPF = finishGPF;

        if (divisionNumber < 1)
            this.divisionNumber = 1;
        else
            this.divisionNumber = divisionNumber;

        this.pathToFiles = pathToFiles;
        this.optionalString = optionalString;
    }

    public String getPathToFiles()
    {
        return pathToFiles;
    }

    public void setPathToFiles(String pathToFiles)
    {
        this.pathToFiles = pathToFiles;
    }

    public String getReceptor()
    {
	return receptor;
    }

    public void setReceptor(String receptor)
    {
	this.receptor = receptor;
    }

    public String getLigand()
    {
	return ligand;
    }

    public void setLigand(String ligand)
    {
	this.ligand = ligand;
    }

    public String getReceptorFlexiblePart()
    {
	    return receptorFlexiblePart;
    }

    public void setReceptorFlexiblePart(String receptorFlexiblePart)
    {
	    this.receptorFlexiblePart = receptorFlexiblePart;
    }

    public String getStartGPF()
    {
	return startGPF;
    }

    public void setStartGPF(String startGPF)
    {
	this.startGPF = startGPF;
    }

    public String getFinishGPF()
    {
	return finishGPF;
    }

    public void setFinishGPF(String finishGPF)
    {
	this.finishGPF = finishGPF;
    }

    public int getDivisionNumber()
    {
	return divisionNumber;
    }

    public void setDivisionNumber(int l)
    {
	this.divisionNumber = l;
    }

    public String getOptionalString()
    {
        return optionalString;
    }

    public void setOptionalString(String optionalString)
    {
        this.optionalString = optionalString;
    }

    /**@return Возвращает идентификатор задания:рецептор_лиганд_гибкая часть рецептора_число делений*/
    public String getId()
    {
	return receptor.replaceAll("\\.","_") + "_" + ligand.replaceAll("\\.","_")
            + "_" + receptorFlexiblePart.replaceAll("\\.","_") + "_" + divisionNumber;
    }

    @Override
    public void readFields(DataInput arg0) throws IOException
    {
        receptor = arg0.readUTF();
        ligand = arg0.readUTF();
        receptorFlexiblePart = arg0.readUTF();
        startGPF = arg0.readUTF();
        finishGPF = arg0.readUTF();
        divisionNumber = arg0.readInt();
        pathToFiles = arg0.readUTF();
        optionalString = arg0.readUTF();
	
    }
    @Override
    public void write(DataOutput arg0) throws IOException
    {
        arg0.writeUTF(receptor);
        arg0.writeUTF(ligand);
        arg0.writeUTF(receptorFlexiblePart);
        arg0.writeUTF(startGPF);
        arg0.writeUTF(finishGPF);
        arg0.writeInt(divisionNumber);
        arg0.writeUTF(pathToFiles);
        arg0.writeUTF(optionalString);
    }

    /**
     * @return путь к файлу рецептора
     */
    public String getReceptorPath()
    {
	return pathToFiles + "/" + receptor;
    }
    /**
     * @return путь к файлу лиганда
     */
    public String getLigandPath()
    {
	return  pathToFiles + "/" + ligand;
    }

    /**
     * @return путь к файлу гибкой части рецептора
     */
    public String getReceptorFlexiblePartPath()
    {
	    if (receptorFlexiblePart.equals(""))
	        return receptorFlexiblePart;
        return  pathToFiles + "/" + receptorFlexiblePart;
    }
    /**
     * @return путь к стартовому GPF
     */
    public String getStartGPFPath()
    {
	    return  pathToFiles + "/" + startGPF;
    }
    /**
     * @return путь к конечному GPF
     */
    public String getFinishGPFPath()
    {
	return  pathToFiles + "/" + finishGPF;
    }

    @Override
    public String toString() {
        String m = "DockingProperties:" + getId() +";"+"\n" +
                    "Receptor:" + receptor + ";"+"\n"+
                    "Ligand:" + ligand + ";"+"\n"+
                    "ReceptorFlexPart:" + receptorFlexiblePart + ";"+"\n" +
                    "Start:" + startGPF + ";"+"\n" +
                    "Finish:" + finishGPF + ";"+"\n" +
                    "Divisions:" + divisionNumber + ";"+"\n" +
                    "path to files:" + pathToFiles + ";"+"\n" +
                    "optional:" + optionalString + ";"+"\n" ;
        return m;
    }
}
