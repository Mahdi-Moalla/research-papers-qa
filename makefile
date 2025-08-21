markdown_source1=https://raw.githubusercontent.com/weiaicunzai/awesome-image-classification/refs/heads/master/README.md
markdown_tag1=image-classification

markdown_source2=https://raw.githubusercontent.com/abacaj/awesome-transformers/refs/heads/main/README.md
markdown_tag2=transformers

markdown_source3=https://raw.githubusercontent.com/Hannibal046/Awesome-LLM/refs/heads/main/README.md
markdown_tag3=llm

data_ingestion_dir=./data/raw

data-extract:
	python data_pipeline/data_extract.py process_markdown \
		${markdown_source3} ${markdown_tag3}

init-dirs:
	mkdir -p data
	mkdir -p data/mongodb 
	sudo chmod 777 ./data/mongodb

docker-compose:
	docker compose up --remove-orphans --force-recreate

mongosh:
	docker run --rm -it --network host  \
		mongodb/mongodb-community-server:7.0-ubi9 mongosh mongodb://mongo_user@localhost:27017/
