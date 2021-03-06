# Учебный проект - Аналитика в авиакомпании

## Задачи

- Написать парсер для сбора с сайта данных о 10 крупнейших фестивалях 2018 года (код парсинга представлен в файле request.py);
- Найти количество рейсов с вылетом в сентябре 2018 года на каждой модели самолёта;
- Посчитать количество рейсов по всем моделям самолетов Boeing и Airbus в сентябре;
- Посчитать среднее количество прибывающих рейсов в день для каждого города за август 2018 года;
- Установить фестивали, которые проходили с 23 июля по 30 сентября 2018 года в Москве, и номер недели, в которую они проходили;
- Для каждой недели с 23 июля по 30 сентября 2018 года посчитать количество билетов, купленных на рейсы в Москву;
- Выбрать топ-10 городов по количеству рейсов;
- Построить графики: модели самолетов и количество рейсов, города и количество рейсов, топ-10 городов и количество рейсов.


## Используемые библиотеки:
- *pandas*
- *pymystem3*
- *datetime*
- *matplotlib*
- *seaborn*
- *numpy*
- *stats*

## Данные

Для анализа были доступны данные из базы об авиаперевозках:

- информация об аэропортах:
  - трёхбуквенный код аэропорта;
  - название аэропорта;
  - город;
  - временная зона;
  
- информация о самолётах:
  - код модели самолёта;
  - модель самолёта;
  - количество самолётов;
  
- информация о билетах:
  - уникальный номер билета;
  - персональный идентификатор пассажира;
  - имя и фамилия пассажира;
  
- информация о рейсах:
  - уникальный идентификатор рейса;
  - аэропорт вылета;
  - дата и время вылета;
  - аэропорт прилёта;
  - дата и время прилёта;
  – id самолёта;
  
- стыковая таблица «рейсы-билеты»:
  - номер билета;
  - идентификатор рейса;
  
- информация о фестивалях:
  - уникальный номер фестиваля;
  - дата проведения фестиваля;
  - город проведения фестиваля;
  - название фестиваля.