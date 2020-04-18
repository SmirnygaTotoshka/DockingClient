/**
 * 
 */
package ru.smirnygatotoshka.exception;

/**
 * Бросается при попытке запуска подвижной задачи(DockingProperties.divisionNumber > 1),но с одинаковыми конечными и стартовыми GPF
 * @author SmirnygaTotoshka
 *
 */
public class MovingDockingException extends Exception {

	/**
	 * 
	 */
	public MovingDockingException() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param arg0
	 */
	public MovingDockingException(String arg0) {
		super(arg0);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param arg0
	 */
	public MovingDockingException(Throwable arg0) {
		super(arg0);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param arg0
	 * @param arg1
	 */
	public MovingDockingException(String arg0, Throwable arg1) {
		super(arg0, arg1);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param arg0
	 * @param arg1
	 * @param arg2
	 * @param arg3
	 */
	public MovingDockingException(String arg0, Throwable arg1, boolean arg2, boolean arg3) {
		super(arg0, arg1, arg2, arg3);
		// TODO Auto-generated constructor stub
	}

}
