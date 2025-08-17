markdown_source=https://raw.githubusercontent.com/weiaicunzai/awesome-image-classification/refs/heads/master/README.md
markdown_tag=image-classification

data_ingestion_dir=./data/raw

data-ingestion:
	python data_pipeline/data_extract.py process_markdown \
		${markdown_source} ${data_ingestion_dir} ${markdown_tag} 
