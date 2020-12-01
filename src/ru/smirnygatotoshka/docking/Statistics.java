package ru.smirnygatotoshka.docking;

import ru.smirnygatotoshka.exception.TaskException;

import java.text.DecimalFormat;

public class Statistics {

    private int numTasks;

    public int getNumTasks() {
        return numTasks;
    }

    public void setNumTasks(int numTasks) {
        this.numTasks = numTasks;
    }

    private volatile int all;
    private volatile int executeFail;
    private volatile int analyzeFail;
    private volatile int success;

    public enum Counters {
        SUCCESS,
        ANALYZE_FAIL,
        EXECUTION_FAIL,
        ALL;
    }

    private static Statistics instance;

    public static Statistics getInstance(){
        if (instance == null)
            instance = new Statistics();
        return instance;
    }

    private Statistics()
    {
        all = 0;
        executeFail = 0;
        analyzeFail = 0;
        success = 0;
    }


    public synchronized void incrCounter(Counters counter, int num) throws TaskException {
        switch (counter){
            case SUCCESS:
                success += num;
                break;
            case ANALYZE_FAIL:
                analyzeFail += num;
                break;
            case EXECUTION_FAIL:
                executeFail += num;
                break;
            case ALL:
                all += num;
                break;
            default:
                throw new TaskException("Некорректный счётчик.");
        }
    }

    public int getAll() {
        return all;
    }

    public int getExecuteFail() {
        return executeFail;
    }

    public int getAnalyzeFail() {
        return analyzeFail;
    }

    public int getSuccess() {
        return success;
    }

    @Override
    public String toString(){
        DecimalFormat format = new DecimalFormat("###.##");
        String s, f;
        float s_p,f_p;
        try{
            s_p = (float) success / all * 100;
            f_p = (float) (executeFail + analyzeFail) / all * 100;
            s = format.format(s_p);
            f = format.format(f_p);
        }
        catch (ArithmeticException e){
            s = f ="0.00000";
        }
        return "All = " + all + "\n" +
               "Execute fail = " + executeFail + "\n" +
               "Analyse fail = " + analyzeFail + "\n" +
               "There are failed " + f + "%.\n"+
               "Success = " + success + "; There are " + s + "%.\n";
    }
}