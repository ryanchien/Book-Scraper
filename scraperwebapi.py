from flask import Flask, request, jsonify, abort
import urllib.parse
import pymongo
import os
import json

MDB_USER = urllib.parse.quote_plus(os.getenv("MDB_USER"))
MDB_PW = urllib.parse.quote_plus(os.getenv("MDB_PW"))

app = Flask(__name__)

client = pymongo.MongoClient("mongodb+srv://%s:%s@booksauthors-9srhh.mongodb.net/test?retryWrites=true&w=majority" %(MDB_USER, MDB_PW))
db = client["Data"]
books_collection = db["Books"]
authors_collection = db["Authors"]


@app.route("/", methods=["GET"])
def home():
    return '''<h1>Assignment 2.1</h1>
<p>hello world</p>'''


@app.route("/api/books", methods=["GET", "PUT", "POST", "DELETE"])
def api_books():
	if request.method == "GET":
		query_parameters = request.args
		title = query_parameters.get("title")
		authors = query_parameters.get("authors")
		related_books = query_parameters.get("relatedbooks")
		mongo_query = {}
		if title:
			mongo_query = {"title": title.replace("%20", " ")}
		if authors:
			authors_list = authors.replace("%20", " ").split("&20")
			mongo_query = {"authors": {"$all": authors_list}}
		if related_books:
			related_books_list = related_books.replace("%20", " ").split("&20")
			mongo_query = {"similar_books": {"$all": related_books_list}}
		results = {}
		cursor = books_collection.find(mongo_query)
		i=0
		for doc in cursor:
			results["book"+str(i)] = doc
			i+=1
		return json.dumps(results, indent=4) + "\n"

	elif request.method == "PUT":
		if not request.json:
			abort(400)
		if not request.is_json:
			abort(415)
		new_query = {"$set": request.json}
		query_parameters = request.args
		title = query_parameters.get("title")
		authors = query_parameters.get("authors")
		related_books = query_parameters.get("relatedbooks")
		mongo_query = {}
		if title:
			mongo_query = {"title": title.replace("%20", " ")}
		if authors:
			authors_list = authors.replace("%20", " ").split("&20")
			mongo_query = {"authors": {"$all": authors_list}}
		if related_books:
			related_books_list = related_books.replace("%20", " ").split("&20")
			mongo_query = {"similar_books": {"$all": related_books_list}}
		books_collection.update_one(mongo_query, new_query)
		results = {}
		cursor = books_collection.find(request.json)
		i=0
		for doc in cursor:
			results["book"+str(i)] = doc
			i+=1
		return json.dumps(results, indent=4) + "\n"

	elif request.method == "POST":
		if not request.json or not "_id" in request.json or not "rating_count" in request.json \
		or not "title" in request.json  or not "book_url" in request.json or not "author_urls" in request.json \
		or not "image_url" in request.json or not "review_count" in request.json or not "authors" in request.json \
		or not "rating" in request.json or not "similar_books" in request.json or not "ISBN" in request.json:
			abort(400)
		if not request.is_json:
			abort(415)
		bid = request.json.get("_id", "")
		if books_collection.find_one({"_id": bid}) != None:
			abort(400)
		else:
			books_collection.insert_one(request.json)
		return json.dumps(request.json, indent=4) + "\n", 201

	elif request.method == "DELETE":
		query_parameters = request.args
		title = query_parameters.get("title")
		authors = query_parameters.get("authors")
		related_books = query_parameters.get("relatedbooks")
		mongo_query = {}
		if title:
			mongo_query = {"title": title.replace("%20", " ")}
		if authors:
			authors_list = authors.replace("%20", " ").split("&20")
			mongo_query = {"authors": {"$all": authors_list}}
		if related_books:
			related_books_list = related_books.replace("%20", " ").split("&20")
			mongo_query = {"similar_books": {"$all": related_books_list}}
		if books_collection.find_one(mongo_query) != None:
			books_collection.delete_one(mongo_query)
			return {"status": "entry deleted"}, 200
		else:
			abort(400)
		

@app.route("/api/authors", methods=["GET", "PUT", "POST", "DELETE"])
def api_authors():
	if request.method == "GET":
		query_parameters = request.args
		name = query_parameters.get("name")
		book_title = query_parameters.get("booktitle")
		mongo_query = {}
		if name:
			mongo_query = {"name": name.replace("%20", " ")}
		if book_title:
			book_title_list = book_title.replace("%20", " ").split("&20")
			mongo_query = {"author_books": {"$all": book_title_list}}
		results = {}
		cursor = authors_collection.find(mongo_query)
		i=0
		for doc in cursor:
			results["author"+str(i)] = doc
			i+=1
		return json.dumps(results, indent=4) + "\n"

	elif request.method == "PUT":
		if not request.json:
			abort(400)
		if not request.is_json:
			abort(415)
		new_query = {"$set": request.json}

		query_parameters = request.args
		name = query_parameters.get("name")
		book_title = query_parameters.get("booktitle")
		mongo_query = {}
		if name:
			mongo_query = {"name": name.replace("%20", " ")}
		if book_title:
			book_title_list = book_title.replace("%20", " ").split("&20")
			mongo_query = {"author_books": {"$all": book_title_list}}
		authors_collection.update_one(mongo_query, new_query)
		results = {}
		cursor = authors_collection.find(request.json)
		i=0
		for doc in cursor:
			results["author"+str(i)] = doc
			i+=1
		return json.dumps(results, indent=4) + "\n"

	elif request.method == "POST":
		if not request.json or not "_id" in request.json or not "rating_count" in request.json \
		or not "name" in request.json  or not "author_books" in request.json or not "author_url" in request.json \
		or not "image_url" in request.json or not "review_count" in request.json or not "rating" in request.json \
		or not "similar_authors" in request.json:
			abort(400)
		if not request.is_json:
			abort(415)
		aid = request.json.get("_id", "")
		if authors_collection.find_one({"_id": aid}) != None:
			abort(400)
		else:
			authors_collection.insert_one(request.json)
		return json.dumps(request.json, indent=4) + "\n", 201

	elif request.method == "DELETE":
		query_parameters = request.args
		name = query_parameters.get("name")
		book_title = query_parameters.get("booktitle")
		mongo_query = {}
		if name:
			mongo_query = {"name": name.replace("%20", " ")}
		if book_title:
			book_title_list = book_title.replace("%20", " ").split("&20")
			mongo_query = {"author_books": {"$all": book_title_list}}
		if authors_collection.find_one(mongo_query) != None:
			authors_collection.delete_one(mongo_query)
			return {"status": "entry deleted"}, 200
		else:
			abort(400)


if __name__ == "__main__":
	app.run()