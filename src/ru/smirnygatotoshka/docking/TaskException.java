package ru.smirnygatotoshka.docking;
/**
 * Бросается при некорректном завершении задания
 * @author SmirnygaTotoshka
 * */
public class TaskException extends Exception {


	public TaskException(String string) {
		super(string);
	}

}
