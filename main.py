from flask import Flask, request, render_template, Markup, jsonify
from functions import update_holders_list, get_holders_list, update_excluded_addresses
import json
import os

app = Flask(__name__)

SECRET_KEY = os.getenv("SECRET_KEY")


@app.route("/update", methods=["POST"])
def update():
    if request.method == "POST":
        params = request.form
        key = params.get("key")
        if key == SECRET_KEY:
            excluded_addresses = params.get("excluded_addresses")
            if excluded_addresses is not None:
                excluded_addresses = json.loads(excluded_addresses)
                print(excluded_addresses)
                update_excluded_addresses(excluded_addresses)
            result = update_holders_list()
            return result
        return "ERROR"


@app.route('/')
def html_content():
    content = Markup(get_holders_list("html"))
    return render_template("index.html", holders_table=content)


@app.route("/json")
def json_content():
    content = get_holders_list("json")
    return content


host = os.getenv("HOST")
port = os.getenv("PORT")

app.run(host=host, port=port, debug=True)
