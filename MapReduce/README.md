# OLAP - Map Reduce
Сделал все этапы по примеру HIVE.

Разделил задачу на четыре последовательных запроса:

1. QuestionsFilter.java - Отбираем только вопросы с тегом hadoop. *Time taken: 2m3s*
2. QuestAnswJoin.java - Отбираем подходящие ответы с рейтингом. *Time taken: 7m32s*
3. UserFilter.java - Отбираем только пользователей из России. *Time taken: 21s*
4. HadoopRussia.java -  Соединяем таблицы два и три, сортируем, выбираем 50 первых. *Time taken: 26s*

**Total time taken: 10m22s seconds**