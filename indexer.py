import nltk
from nltk.stem.snowball import EnglishStemmer
import pickle
import os
from bs4 import BeautifulSoup
from tqdm import tqdm
import json
from posting import Posting
import shutil
from utils import parse_config, process_token, parse_multiple_posting
from urllib.parse import urldefrag


class Indexer:
    """NLTK-based inverted indexer"""

    def __init__(self, use_stemmer=False):
        self.tokenizer = nltk.word_tokenize  # sentence tokenization models
        self.stemmer = None
        if use_stemmer:
            self.stemmer = EnglishStemmer()  # slightly better than Porter stemmer

        # Map from document ID to its URL
        self.doc_id_url_map = {}
        self.doc_id_disk_loc = {}

        # Map from term to its location in final posting file
        self.term_posting_map = {}

        # Map from term to its file, used for partial index
        self.__term_file_partial_map = {}
        self.__current_partial_index_file_id = 0

        # Set of discovered URL, maybe useful for duplication, now contains URL without fragment
        self.__discovered_url = set()

        # all term posting location
        self.term_posting_path = None

        # Map between terms and its posting.
        # Posting is currently
        self.inverted_index = {}

        self.__current_id = 0
        self.__current_approximate_size = 0

    def index_document(self, document, url, disk_location, temp_dir, partial_max_size):
        # Defragment as soft duplication detection
        defragmented_url, _ = urldefrag(url)
        if defragmented_url in self.__discovered_url:  # skip if already exists
            return
        self.__discovered_url.add(defragmented_url)

        # Process docID mapping
        doc_id = self.__current_id
        # Doc ID mapping
        self.doc_id_url_map[doc_id] = defragmented_url
        self.doc_id_disk_loc[doc_id] = disk_location
        self.__current_id += 1

        # posting for this doc
        doc_posting_dict = {}

        token_pos = 0
        for token in self.tokenizer(document):
            token = process_token(token, self.stemmer)
            if not token:
                continue

            if token not in doc_posting_dict:
                doc_posting_dict[token] = Posting()

            doc_posting_dict[token].doc_id = doc_id
            doc_posting_dict[token].update_position_list(token_pos)
            token_pos += 1

        self.update_inverted_index(doc_posting_dict, temp_dir, partial_max_size)

    # update posting for current document into list of inverted_index posting
    # guarantee posting is sorted
    def update_inverted_index(self, doc_posting, temp_dir, partial_max_size):
        for term, posting in doc_posting.items():
            if term not in self.inverted_index:
                self.inverted_index[term] = []
            self.inverted_index[term].append(posting)
            self.__current_approximate_size += posting.get_approximate_size()

        if self.__current_approximate_size > partial_max_size:
            self.save_partial_index(temp_dir)

    def save_partial_index(self, temp_dir):
        if len(self.inverted_index) == 0:
            return
        os.makedirs(temp_dir, exist_ok=True)
        tmp_partial_path = os.path.join(temp_dir, f"partial_tmp_{self.__current_partial_index_file_id:06}")
        with open(tmp_partial_path, "w") as f:
            for term, postings in self.inverted_index.items():
                posting_start = f.tell()
                for posting in postings:
                    f.write(str(posting) + "\n")
                posting_length = f.tell() - posting_start
                if term not in self.__term_file_partial_map:
                    self.__term_file_partial_map[term] = []

                self.__term_file_partial_map[term].append((self.__current_partial_index_file_id,
                                                           posting_start,
                                                           posting_length))
        self.__current_partial_index_file_id += 1
        self.inverted_index = {}                # Reset inverted index
        self.__current_approximate_size = 0     # Reset processing size

    # Merge partial index
    def merge_and_write_partial_posting(self, path_to_dump, tmp_dir):
        # Writing any posting left in inverted index
        self.save_partial_index(tmp_dir)

        tqdm.write("Merging partial index")
        # Open final posting file to write
        with open(path_to_dump, "w") as f_out:
            for term, postings in tqdm(self.__term_file_partial_map.items()):
                final_term_posting_list = []
                # Get partial posting from multiple files
                for file_idx, posting_start, posting_length in postings:
                    partial_file_path = os.path.join(tmp_dir, f"partial_tmp_{file_idx:06}")
                    # f.seek problems with new line on Windows, so we have to read it in binary mode and decode the str
                    with open(partial_file_path, "rb") as f_in:
                        f_in.seek(posting_start)
                        content = f_in.read(posting_length)
                        final_term_posting_list.extend(parse_multiple_posting(content.decode("utf-8")))

                 # Write to final file. Mapping term to its posting position in final merged file
                posting_start = f_out.tell()
                for posting in final_term_posting_list:
                    f_out.write(str(posting) + "\n")
                posting_length = f_out.tell() - posting_start
                self.term_posting_map[term] = (posting_start, posting_length)

    def dump_indexer_state(self, dir_to_dump, doc_id_file, all_posting_file, term_posting_file, tmp_dir):
        os.makedirs(dir_to_dump, exist_ok=True)

        print("Saving doc_id map")
        self.dump_doc_id_map(os.path.join(dir_to_dump, doc_id_file))
        print("Merging and writing partial posting")
        self.merge_and_write_partial_posting(os.path.join(dir_to_dump, all_posting_file), tmp_dir)
        print("Saving term posting map")
        self.dump_term_posting_map(os.path.join(dir_to_dump, term_posting_file))  # Save this last
        print(f"Done. Indexer state dumped to: {dir_to_dump}")

    def dump_term_posting_map(self, path_to_dump):
        with open(path_to_dump, "wb") as f_out:
            pickle.dump(self.term_posting_map, f_out, pickle.HIGHEST_PROTOCOL)

    def dump_doc_id_map(self, path_to_dump):
        doc_id_state = {
            "url_map": self.doc_id_url_map,
            "disk_loc": self.doc_id_disk_loc
        }
        with open(path_to_dump, "wb") as f_out:
            pickle.dump(doc_id_state, f_out, pickle.HIGHEST_PROTOCOL)


def create_indexer(data_path, temp_dir, partial_max_size, use_stemmer=False):
    assert os.path.exists(data_path), "Input path does not exist"

    indexer = Indexer(use_stemmer)  # For now let's not use stemmer
    for directory in tqdm(os.listdir(data_path), desc="Whole dataset progress"):
        for file in tqdm(
            os.listdir(os.path.join(data_path, directory)),
            leave=False,
            desc=f"Processing {directory}",
        ):
            file_path = os.path.join(data_path, directory, file)
            with open(file_path) as f:
                content = json.load(f)

            soup = BeautifulSoup(content["content"], "lxml")
            # TODO: Right now all text have equal weight. Need to change importance for title, h1, h2, etc.
            indexer.index_document(soup.text, content["url"], os.path.abspath(file_path), temp_dir, partial_max_size)

    return indexer


if __name__ == "__main__":
    default_config, data_config = parse_config()

    tmp_dir = default_config["tmp_dir"]
    # Delete partial index before creating indexer
    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir)

    index_db = create_indexer(data_config["data_path"],
                              tmp_dir,
                              int(float(data_config["partial_max_size"])))

    index_db.dump_indexer_state(data_config["indexer_state_dir"],
                                default_config["doc_id_file"],
                                default_config["all_posting_file"],
                                default_config["term_posting_map_file"],
                                tmp_dir)
