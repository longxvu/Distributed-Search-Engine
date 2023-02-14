import flask
from retriever import Retriever
import time
from flask import render_template, request
from utils import parse_config, generate_result_pages


default_config, data_config = parse_config()
app = flask.Flask(__name__,
                  static_folder=default_config["static_dir"],
                  template_folder=default_config["template_dir"])
app.config["DEBUG"] = True

# Load indexer
indexer = Retriever(data_config["indexer_state_dir"],
                    default_config["doc_id_file"],
                    default_config["all_posting_file"],
                    default_config["term_posting_map_file"])


@app.route("/", methods=["GET"])
def home():
    return render_template("search_engine.html", data=None)


@app.route("/search", methods=["POST"])
def search():
    query = request.form.get("input")
    print(query)
    start = time.time()
    url_map, disk_locs = indexer.retrieve(query, top_k=int(default_config["max_result"]))
    ts = f"Retrieval took {(time.time() - start):.3f}s"

    generated_results = generate_result_pages(disk_locs,
                                              default_config["static_dir"],
                                              default_config["generated_results_dir"],
                                              query)
    print(generated_results)
    data = {
        "results": list(zip(url_map, generated_results)),
        "ts": ts
    }
    print(data)
    return render_template("search_engine.html", data=data)


app.run()
