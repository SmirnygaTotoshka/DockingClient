package ru.smirnygatotoshka.docking;

import org.apache.hadoop.io.Text;

/**
 * Считывает данные из входного файла, о каждом задании
 * @author SmirnygaTotoshka
 * */
public class ConfigParser 
{
//TODO - delete

	/**
	 * Считывает из строки данные о задании
	 *
	 * @param line   - строка из исходного файла.
	 * @param client - объект, ответственный за отправку сообщений на сервер
	 * @return Класс, содержащий информацию о задании
	 */
	public Dock parseDock(Text line) {
		String[] description = line.toString().split(",");

		DockingProperties properties = new DockingProperties();

		properties.setPathToFiles(description[0]);
		properties.setReceptor(description[1]);
		properties.setReceptorFlexiblePart(description[2]);
		properties.setLigand(description[3]);
		properties.setStartGPF(description[4]);
		properties.setFinishGPF(description[5]);
		properties.setDivisionNumber(Integer.parseInt(description[6]));
		properties.setOptionalString(description[7]);

		Dock dock = new Dock(properties,parameters);
		dock.setClient(client);
		return dock;
	}

}
