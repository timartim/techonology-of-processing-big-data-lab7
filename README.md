# OpenFoodFacts KMeans Clustering on PySpark

Проект для кластеризации продуктов из OpenFoodFacts с помощью `PySpark` и алгоритма `KMeans`.

## Что делает проект

Проект:

- читает данные из `.parquet`
- автоматически выбирает числовые признаки с достаточной заполненностью
- выполняет предобработку данных
- масштабирует признаки
- обучает несколько моделей `KMeans` для разных `k`
- выбирает лучшую модель по метрике `silhouette`
- сохраняет:
  - CSV с кластерами
  - CSV с профилями кластеров
  - CSV с центрами кластеров
  - JSON с метриками
  - артефакты модели в папку `models`

## Основные команды
#### установка зависимостей
```bash
pip install -r requirements.txt
```
#### Установка данных
В папку src/data требуется положить файл с именем food_small.parquet, модель будет обучаться на них. 
Другой путь можно описать в файле src/spark/config.json в поле input_path. Путь до данных должен быть относительным. 

#### Обучение модели
```bash
python src/spark/main.py train
```
После обучения будут сохранены:

- src/artifacts/food_clusters.csv
- src/artifacts/food_cluster_profiles.csv
- src/artifacts/food_cluster_centers.csv
- src/artifacts/food_metrics.json
- src/models/openfoodfacts_kmeans/

#### Предсказание новых данных

```bash
python src/spark/main.py predict \
  --model-path ../models/openfoodfacts_kmeans \
  --input ../data/food_small.parquet \
  --output ../artifacts/predictions.csv
```

Результирующий файл записывается в --output.

## Запуск через Docker

Перед запуском убедитесь, что файл с данными находится в папке `src/data` и имеет имя `food_small.parquet`:

```text
src/data/food_small.parquet
```

### 1. Поднять MongoDB и Web UI

Сначала необходимо запустить MongoDB и Mongo Express:

```bash
docker compose up -d mongo mongo-express
```

MongoDB будет доступна:

- внутри Docker-сети: `mongodb://mongo:27017/openfoodfacts`
- с хоста: `mongodb://localhost:27017/openfoodfacts`

Mongo Express Web UI:

```text
http://localhost:8081
```

Web UI открывается в браузере по адресу `localhost:8081`.

Логин и пароль:

```text
admin
admin
```

### Scala-витрина данных

В проект добавлена минимальная витрина данных на Scala в папке `src/datamart`.
Она поднимает HTTP API поверх MongoDB и задает единый JSON-формат обмена между моделью и источником данных. На этом шаге витрина не выполняет предобработку признаков: она только принимает запросы, читает/пишет документы и сохраняет результаты работы модели.

Запуск витрины через Docker Compose:

```bash
docker compose up -d mongo datamart
```

Проверка доступности:

```bash
curl http://localhost:8090/health
```

Единый формат ответа:

```json
{
  "status": "ok",
  "data": {}
}
```

При ошибке:

```json
{
  "status": "error",
  "error": "Описание ошибки"
}
```

Основные маршруты витрины:

- `POST /v1/source/training/query` — получить данные для обучения из источника;
- `POST /v1/model-results/training` — сохранить результаты обучения модели;
- `POST /v1/model-results/predictions` — сохранить результаты тестирования/предсказания модели.

Пример запроса к источнику:

```bash
curl -X POST http://localhost:8090/v1/source/training/query \
  -H 'Content-Type: application/json' \
  -d '{"fields":["product_name","energy-kcal_100g"],"limit":10}'
```

Пример сохранения результатов предсказания:

```bash
curl -X POST http://localhost:8090/v1/model-results/predictions \
  -H 'Content-Type: application/json' \
  -d '{
    "sourcePath": "../data/food_small.parquet",
    "predictions": [
      {
        "product_name": "Example product",
        "prediction": 1
      }
    ]
  }'
```

Следующий шаг архитектурно простой: заменить прямые вызовы `MongoStorage` в Python-модели на HTTP-вызовы к этой витрине.

### 2. Запустить контейнер с моделью

Контейнер модели вынесен в профиль `model`, чтобы обычный `docker compose up` поднимал только MongoDB, Mongo Express и витрину данных.
Перед запуском модели в папке `src/data` должен быть файл:

```text
src/data/food_small.parquet
```

После этого можно запустить обучение:

```bash
docker compose --profile model run --rm app python main.py train
```

При запуске обучения приложение:

1. читает `src/data/food_small.parquet`;
2. выгружает содержимое parquet в MongoDB, коллекция `training_data`;
3. загружает данные для обучения из MongoDB;
4. обучает модель `KMeans`;
5. сохраняет результаты обучения в файлы и MongoDB.

Результаты в MongoDB сохраняются в коллекции:

- `training_data`
- `imports`
- `training_clusters`
- `cluster_profiles`
- `cluster_centers`
- `training_metrics`
- `model_info`

### 3. Остановить контейнеры

После работы можно остановить MongoDB и Mongo Express:

```bash
docker compose down
```

### Полезные команды

Проверить состояние контейнеров:

```bash
docker compose ps
```

Посмотреть логи приложения:

```bash
docker compose logs -f app
```

Посмотреть логи Mongo Express:

```bash
docker compose logs -f mongo-express
```

## Настройки

Настройки spark и гиперпараметры алгоритма находятся в по пути
```bash
src/spark/config.json 
```

## Требования

- Python 3.10+
- Java 17 или 21
- установленный и работающий PySpark
