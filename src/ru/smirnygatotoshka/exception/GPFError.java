/**
 * 
 */
package ru.smirnygatotoshka.exception;

/**
 * Бросается при ошибке формирования GPF
 * @author SmirnygaTotoshka
 *
 */
public class GPFError extends Error {

	/**
	 * 
	 */
	public GPFError() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 */
	public GPFError(String message) {
		super(message);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param cause
	 */
	public GPFError(Throwable cause) {
		super(cause);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 * @param cause
	 */
	public GPFError(String message, Throwable cause) {
		super(message, cause);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 * @param cause
	 * @param enableSuppression
	 * @param writableStackTrace
	 */
	public GPFError(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
		// TODO Auto-generated constructor stub
	}

}
