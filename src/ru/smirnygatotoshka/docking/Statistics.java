package ru.smirnygatotoshka.docking;

import java.text.DecimalFormat;

public class Statistics {

    private volatile int all;
    private volatile int failed;
    private volatile int success;

    public enum Counters {
        SUCCESS,
        FAILED,
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
        failed = 0;
        success = 0;
    }


    public synchronized void incrCounter(Counters counter, int num) throws TaskException {
        switch (counter){
            case SUCCESS:
                success += num;
                break;
            case FAILED:
                failed += num;
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

    public int getFailed() {
        return failed;
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
            f_p = (float) failed / all * 100;
            s = format.format(s_p);
            f = format.format(f_p);
        }
        catch (ArithmeticException e){
            s = f ="0.00000";
        }
        return "All = " + all + "\n" +
               "Failed = " + failed + "\n" +
               "There are failed " + f + "%.\n"+
               "Success = " + success + "; There are " + s + "%.\n";
    }
}
