from flask import Flask, redirect, render_template, request, jsonify

from searching import search_api, random_api, search_images

app = Flask(__name__, template_folder="../templates", static_folder="../static")

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/search", methods=["GET", "POST"])
def search():
    search = request.args.get("search", "")

    if request.method == "POST":
        page = int(request.args.get("page", ""))
        results, page_count = search_api(search, page)
        return jsonify({"data": results, "page_count": page_count}), 201

    results, page_count = search_api(search)

    return render_template("search.html", search=search, results=results, page_count=page_count)

@app.route("/lucky")
def lucky():
    random_page = random_api()

    return redirect(random_page)

@app.route("/images", methods=["GET", "POST"])
def images():
    search = request.args.get("search", '')

    if request.method == "POST":
        data = request.get_json()
        images = search_images(search, data["page"])
        return jsonify({"data": images}), 201

    images = search_images(search)

    return render_template("search.html", images=images, search=search)
