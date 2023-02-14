## Prerequisites
Run these commands to install needed packages
```shell
pip install -r packages/requirements.txt
python packages/nltk_requirement_installation.py
```

## Write indexer to file
Assuming data has this structure:
```shell
.
└── search_engine
    ├── data
        ├── ANALYST
        └── DEV
    ├── packages
    └── indexer.py
    └── retriever.py
    └── ...
```
```shell
# default for analyst dataset (for faster testing)
python indexer.py

# for dev dataset, can change data argument to any path
python indexer.py --data data/DEV
```

## Running search engine
Assuming inverted index and doc id map is obtained

```python run.py```: Terminal search engine

```python app.py```: Simple web version

## File function
```indexer.py```: Build inverted index from data and save for easy
retrieval purpose

```retriever.py```: Retrieve query from built index using boolean retrieval model.
All results are sorted by tf-idf score.