# DockingClient

Консольная версия запуска докинга в autodock 4.2 на кластере Hadoop. Позволяет управлять запуском многих тысяч запусков. Как правило запускал по 30000 за раз, считалось 2 дня.
В итоге в hdfs сохраняется dlg файл и таблица с id-значение энергии.
Запускалась на кластере под OS Ubuntu, 15 узлов.