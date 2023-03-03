import pickle
import nltk
import os
from utils import parse_multiple_posting, process_token
from typing import List
import math


class Retriever:
    def __init__(self, index_dir, doc_id_file, all_posting_file, term_posting_file):
        self.tokenizer = nltk.word_tokenize
        self.term_posting_path = None
        self.term_posting_map = None
        self.doc_id_disk_loc = None
        self.doc_id_url_map = None
        self.total_num_doc = 0
        self.load_indexer_state(index_dir, doc_id_file, all_posting_file, term_posting_file)
        self.stemmer = None  # should be consistent with indexer stemmer

    def load_indexer_state(self, dir_to_load, doc_id_file, all_posting_file, term_posting_file):
        print("Loading doc_id map")
        self.load_doc_id_map(os.path.join(dir_to_load, doc_id_file))
        print("Loading term posting map")
        self.load_term_posting_map(os.path.join(dir_to_load, term_posting_file))
        self.term_posting_path = os.path.join(dir_to_load, all_posting_file)

    def load_term_posting_map(self, path_to_load):
        with open(path_to_load, "rb") as f_in:
            self.term_posting_map = pickle.load(f_in)

    def load_doc_id_map(self, path_to_load):
        with open(path_to_load, "rb") as f_in:
            doc_id_state = pickle.load(f_in)
        self.doc_id_url_map = doc_id_state["url_map"]
        self.doc_id_disk_loc = doc_id_state["disk_loc"]
        self.total_num_doc = len(self.doc_id_url_map)

    def load_posting_from_disk(self, term):
        if term not in self.term_posting_map:
            return None
        posting_start, posting_length = self.term_posting_map[term]
        with open(self.term_posting_path, "rb") as f:
            f.seek(posting_start)
            content = f.read(posting_length)
        postings = parse_multiple_posting(content.decode("utf-8"))
        return postings

    def ranked_retrieval(self, processed_query: List[str]):
        doc_tf_idf_map = {}  # key: Document ID, value: TF-IDF value of common terms between query and doc
        term_doc_id_map = {}
        
        # Get documents ID that are associated with this term
        for token in processed_query:
            posting_found = self.load_posting_from_disk(token)
            if posting_found:
                term_doc_id_map[token] = posting_found

        if not term_doc_id_map:
            return []
        

        for term, posting_lst in term_doc_id_map.items():
            for posting in posting_lst:
                doc_tf = 1 + math.log10(posting.term_freq)
                doc_idf = math.log10(self.total_num_doc / len(posting_lst))
                doc_tf_idf = doc_tf * doc_idf
                doc_tf_idf_map[posting.doc_id] = doc_tf_idf_map.get(posting.doc_id, 0) + doc_tf_idf
        
        # doc_tf_idf_map = {k: v for k, v in sorted(doc_tf_idf_map.items(), key=lambda x: x[1], reverse=True)}
        # print(list(doc_tf_idf_map.items())[:100])
        return doc_tf_idf_map
    
    def term_ranked_retrieval(self, term: str):
        doc_tf_idf_map = {}  # key: Document ID, value: TF-IDF value of common terms between query and doc
        term_doc_id_map = None
        
        # Get documents ID that are associated with this term
        term_all_postings = self.load_posting_from_disk(term)

        if not term_doc_id_map:
            return []
        
        for posting in term_all_postings:
            doc_tf = 1 + math.log10(posting.term_freq)
            doc_idf = math.log10(self.total_num_doc / len(term_all_postings))
            doc_tf_idf_map[posting.doc_id] = doc_tf * doc_idf
        
        # doc_tf_idf_map = {k: v for k, v in sorted(doc_tf_idf_map.items(), key=lambda x: x[1], reverse=True)}
        # print(list(doc_tf_idf_map.items())[:100])
        return doc_tf_idf_map



    # boolean retrieval model:
    def boolean_retrieval(self, processed_query: List[str]):
        doc_id_lst = []
        term_doc_id_lst = []
        for token in processed_query:
            # term_doc_id_lst.append([item[0] for item in self.inverted_index[token]])
            # print(self.load_posting_from_disk(token))
            # term_doc_id_lst.append(self.inverted_index[token])
            posting_found = self.load_posting_from_disk(token)
            if posting_found:
                term_doc_id_lst.append(posting_found)

        # No results found for given query
        if len(term_doc_id_lst) == 0:
            return []
        # sort query based on length of its posting list
        term_doc_id_lst = sorted(term_doc_id_lst, key=lambda x: len(x))
        print(term_doc_id_lst)
        pointer_lst = [0 for _ in range(len(term_doc_id_lst))]  # list for skip pointer
        N = len(self.doc_id_url_map)
        # Process term with the lowest amount of doc id
        for posting in term_doc_id_lst[0]:
            current_doc_id = posting.doc_id
            same_doc_id = True

            for term_idx in range(1, len(term_doc_id_lst)):
                # Two condition: Pointer to doc doesn't go out of bound, and other doc id < current doc id
                # Also setting this to -1 in case the first condition fails
                other_doc_id = -1
                while (pointer_lst[term_idx] < len(term_doc_id_lst[term_idx])
                    and (other_doc_id := term_doc_id_lst[term_idx][pointer_lst[term_idx]].doc_id) < current_doc_id):
                    pointer_lst[term_idx] += 1
                # If other term doesn't include the current doc id we can skip this doc
                if other_doc_id != current_doc_id:
                    same_doc_id = False
                    break
            if same_doc_id:
                # term frequency, inverse document frequency
                tfidf = 0
                for token_idx in range(len(term_doc_id_lst)):
                    token_tf = 1 + math.log10(
                        term_doc_id_lst[token_idx][pointer_lst[token_idx]].term_freq
                    )
                    token_idf = math.log10(N / len(term_doc_id_lst[token_idx]))
                    tfidf += token_tf * token_idf
                # for term_idx, doc_ptr in enumerate(pointer_lst):
                #     total_freq += (1 / len(processed_query)) * term_doc_id_lst[term_idx][pointer_lst[term_idx]][1]
                doc_id_lst.append((current_doc_id, tfidf))
            pointer_lst[0] += 1  # increment pointer for first list also
        doc_id_lst = sorted(doc_id_lst, key=lambda x: x[1], reverse=True)
        doc_id_lst = [x[0] for x in doc_id_lst]
        return doc_id_lst

    # Given a string query, break it down into tokens based on our defined tokenizer
    def process_query(self, query: str):
        processed_query = []
        for token in self.tokenizer(query):
            token = process_token(token, self.stemmer)
            if token:
                processed_query.append(token)
        return processed_query

    # Retrieve top k result from search query using boolean retrieval model
    def retrieve(self, query, top_k=5):
        processed_query = self.process_query(query)
        print(processed_query)
        doc_ids = self.ranked_retrieval(processed_query)
        sorted_doc_ids = {k: v for k, v in sorted(doc_ids.items(), key=lambda x: x[1], reverse=True)}
        doc_id_results = [self.doc_id_url_map[doc_id] for doc_id in sorted_doc_ids]
        disk_loc_results = [self.doc_id_disk_loc[doc_id] for doc_id in sorted_doc_ids]
        return doc_id_results[:top_k], disk_loc_results[:top_k]
    
    def retrieve_multi_machine(self, query, top_k=5):
        processed_query = self.process_query(query)
        print(processed_query)

        doc_tfidf_all = []
        # Send each term to the each machine to process independently and getting tfidf map back
        for token in processed_query:
            doc_tfidf_all.append(self.term_ranked_retrieval(token))

        # Get result from all machine and merge into a single tfidf map
        doc_tfidf_map = {}
        for doc_tfidf in doc_tfidf_all:
            for doc_id in doc_tfidf:
                if doc_id not in doc_tfidf_map:
                    doc_tfidf_map[doc_id] = 0
                doc_tfidf_map[doc_id] += doc_tfidf[doc_id]

        doc_tfidf_map = sorted(doc_tfidf_map.items(), key=lambda x: x[1], reverse=True)
        doc_id_results = [self.doc_id_url_map[doc_id] for doc_id in doc_tfidf_map]
        disk_loc_results = [self.doc_id_disk_loc[doc_id] for doc_id in doc_tfidf_map]

        return doc_id_results[:top_k], disk_loc_results[:top_k]


if __name__ == "__main__":
    from utils import parse_config
    import time
    default_config, data_config = parse_config()
    indexer = Retriever(data_config["indexer_state_dir"],
                        default_config["doc_id_file"],
                        default_config["all_posting_file"],
                        default_config["term_posting_map_file"])
    
    input_query = "information learning"
    start = time.time()
    results = indexer.retrieve(input_query, top_k=int(default_config["max_result"]))
    print(f"{time.time() - start:.3f}s")
    print(results)
