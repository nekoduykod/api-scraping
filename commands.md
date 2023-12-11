pip install --no-cache-dir -r requirements.txt

<!-- # pip cache purge -->
pip install --upgrade pip
<!-- pip install poetry -->
<!-- pip install apache-airflow==2.2.3 -->
<!-- pip install apache-airflow-providers-docker  <== apache/airflow:2.2.3 image must have these -->
pip install requests sqlalchemy pandas load_dotenv

docker-compose build 
docker-compose up -d

docker-compose logs
docker-compose ps

docker-compose down