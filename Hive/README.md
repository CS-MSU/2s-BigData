# Big Data - Hive
Схему изучал по ссылке: [stackexchange](https://meta.stackexchange.com/questions/2677/database-schema-documentation-for-the-public-data-dump-and-sede)

Разделил задачу на четыре последовательных запроса:

1. Выбираем все вопрос posttypeid=2, где в столбце tags есть %hadoop%. **Time: 10.45 s**
2. Выбираем все ответы соответствующие вопросам из предыдущего пункта. **Time: 13.55s**
3. Выбираем всех пользователей в поле location которых есть ключевые слова: 'russia' или один из [топ 15](https://en.wikipedia.org/wiki/List_of_cities_and_towns_in_Russia_by_population) городов России по количеству населения. **Time: 14.25s**
4. Соединяем таблицы из пункта 2 и 3 по id, группируем и суммируем поле score, тем самым получаем нашу результирующую таблицу. **Time: 4.87s**


В файле *results_hive.csv* - находятся 50 пользователей с наибольшим score. 

**Total time: 43.2 seconds**