markdown_source=https://raw.githubusercontent.com/weiaicunzai/awesome-image-classification/refs/heads/master/README.md
markdown_tag=image-classification

data_ingestion_dir=./data/raw

# data-ingestion:
# 	docker run --rm --network host  -it  \
# 		-v $(pwd)/data_pipeline:/opt/bitnami/spark/data_pipeline \
# 		bitnami/spark:4.0.0 bash

init-dirs:
	mkdir -p data
	mkdir -p data/mongodb 
	sudo chmod 777 ./data/mongodb

docker-compose:
	docker compose up --remove-orphans --force-recreate

mongosh:
	docker run --rm -it --network host  \
		mongodb/mongodb-community-server:7.0-ubi9 mongosh mongodb://mongo_user@localhost:27017/
