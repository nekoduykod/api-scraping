pip install -r requirements.txt
pip install poetry requests sqlalchemy pandas apache-airflow-providers-docker load_dotenv

docker-compose build 
docker-compose up -d

docker-compose logs
docker-compose ps

docker-compose down