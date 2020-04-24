import os
import pymongo
import urllib.parse
from multiprocessing.dummy import Pool
from webscraper import *
from tests import *

MDB_USER = urllib.parse.quote_plus(os.getenv("MDB_USER"))
MDB_PW = urllib.parse.quote_plus(os.getenv("MDB_PW"))
#STARTING_PAGE = "https://www.goodreads.com/book/show/3735293-clean-code"
STARTING_PAGE = "https://www.goodreads.com/book/show/4671.The_Great_Gatsby"

books_dict = {}
authors_dict = {}
author_urls_to_scrape = []

def connect_to_db():
	"""creates a mongoclient instance and returns books and authors collections"""
	client = pymongo.MongoClient("mongodb+srv://%s:%s@booksauthors-9srhh.mongodb.net/test?retryWrites=true&w=majority" %(MDB_USER, MDB_PW))
	db = client["Data"]
	books_collection = db["Books"]
	authors_collection = db["Authors"]
	return books_collection, authors_collection

def scrape_book(book_url):
	"""book scraping function to be applied using multiprocessing"""
	global books_dict
	global author_urls_to_scrape
	book_id = parse_unique_id_from_url(book_url)
	if book_id in books_dict:
		return {}, []
	driver = new_sel_driver()
	book_urls_to_scrape = []
	try:
		book_data = get_book_data(book_url, driver, book_urls_to_scrape)
	except Exception:
		driver.close()
		return {}, []
	print(book_id)
	try:
		author_urls = book_data["author_urls"]
		for url in author_urls:
			author_urls_to_scrape.append(url)
	except KeyError:
		author_urls = []
	return book_data, book_urls_to_scrape[0]


def scrape_book_par(starting_url, num_processes=4, num_books=200, dump_to_json=True, dump_to_db=True, books_collection=None):
	"""wrapper function to scrape multiple books in parallel"""
	global books_dict
	book_urls_to_scrape = []
	book_data, urls = scrape_book(starting_url)
	book_urls_to_scrape.append(urls)
	if dump_to_db == True:
		try:
			books_collection.insert_one(book_data)
		except pymongo.errors.DuplicateKeyError:
			pass
	book_id = book_data.pop("_id", None)
	if book_id != None:
		books_dict[book_id] = book_data
	i = 0
	while len(books_dict) < num_books:
		with Pool(num_processes) as pool:
			pool_results = pool.map(scrape_book, book_urls_to_scrape[i])
			pool.close()
			pool.join()
		for tup in pool_results:
			book_data = tup[0]
			if len(book_data) > 0:
				if dump_to_db == True:
					bid = book_data["_id"]
					if books_collection.find_one({"_id": bid}) != None:
						pass
					else:
						books_collection.insert_one(book_data)
				book_id = book_data.pop("_id", None)
				if book_id != None:
					books_dict[book_id] = book_data
				book_urls_to_scrape.append(tup[1])
		i += 1
	if dump_to_json == True:
		with open("books.json", 'w', encoding='utf-8') as f:
			json.dump(books_dict, f, indent=4)


def scrape_author(author_url):
	"""author scraping function to be applied using multiprocessing"""
	global authors_dict
	author_id = parse_unique_id_from_url(author_url)
	if author_id in authors_dict:
		return {}
	driver = new_sel_driver()
	try:
		author_data = get_author_data(author_url, driver)
	except Exception:
		driver.close()
		return {}
	print(author_id)
	return author_data


def scrape_author_par(num_processes=4, num_authors=50, dump_to_json=True, dump_to_db=True, authors_collection=None):
	"""wrapper function to scrape multiple authors in parallel"""
	global author_urls_to_scrape
	author_urls = author_urls_to_scrape[:num_authors]
	with Pool(num_processes) as pool:
		pool_results = pool.map(scrape_author, author_urls)
		pool.close()
		pool.join()
	for author_data in pool_results:
		if len(author_data) > 0:
			if dump_to_db == True:
				aid = author_data["_id"]
				if authors_collection.find_one({"_id": aid}) != None:
					pass
				else:
					authors_collection.insert_one(author_data)
			author_id = author_data.pop("_id", None)
			if author_id != None:
				authors_dict[author_id] = author_data
	if dump_to_json == True:
		with open("authors.json", 'w', encoding='utf-8') as f:
			json.dump(authors_dict, f, indent=4)


def test_scrape_one_db(starting_url):
	"""Test scraping one author and book to dump to mongodb"""
	book_data, x = scrape_book(starting_url)
	author_data = scrape_author(author_urls_to_scrape[0])
	if len(book_data) > 0 and len(author_data) > 0:
		bid = author_data["_id"]
		aid = author_data["_id"]
		books_collection, authors_collection = connect_to_db()
		if authors_collection.find_one({"_id": aid}) != None:
			pass
		else:
			authors_collection.insert_one(author_data)
		if books_collection.find_one({"_id": bid}) != None:
			pass
		else:
			books_collection.insert_one(book_data)


def test_scrape_one_json(starting_url):
	"""Test scraping one author and book to dump to json file"""
	book_data, x = scrape_book(starting_url)
	author_data = scrape_author(author_urls_to_scrape[0])
	book_id = book_data.pop("_id", None)
	if book_id != None:
		books_dict[book_id] = book_data
	with open("books.json", 'w', encoding='utf-8') as f:
		json.dump(books_dict, f, indent=4)
	author_id = author_data.pop("_id", None)
	if author_id != None:
		authors_dict[author_id] = author_data
	with open("authors.json", 'w', encoding='utf-8') as f:
		json.dump(authors_dict, f, indent=4)

def test_scrape_one_both(starting_url):
	"""Test scraping one author and book to dump to both mongodb and json file"""
	book_data, x = scrape_book(starting_url)
	author_data = scrape_author(author_urls_to_scrape[0])
	if len(book_data) > 0 and len(author_data) > 0:
		bid = author_data["_id"]
		aid = author_data["_id"]
		books_collection, authors_collection = connect_to_db()
		if authors_collection.find_one({"_id": aid}) != None:
			pass
		else:
			authors_collection.insert_one(author_data)
		if books_collection.find_one({"_id": bid}) != None:
			pass
		else:
			books_collection.insert_one(book_data)
	book_id = book_data.pop("_id", None)
	if book_id != None:
		books_dict[book_id] = book_data
	with open("books.json", 'w', encoding='utf-8') as f:
		json.dump(books_dict, f, indent=4)
	author_id = author_data.pop("_id", None)
	if author_id != None:
		authors_dict[author_id] = author_data
	with open("authors.json", 'w', encoding='utf-8') as f:
		json.dump(authors_dict, f, indent=4)


def test_scrape_mult_json(starting_url):
	"""Test scraping multiple authors and books to dump to json file"""
	scrape_book_par(starting_url, num_processes=5, num_books=30, dump_to_json=True, dump_to_db=False)
	scrape_author_par(num_processes=5, num_authors=5, dump_to_json=True, dump_to_db=False)


def test_scrape_mult_db(starting_url):
	"""Test scraping multiple authors and books to dump to mongodb"""
	books_collection, authors_collection = connect_to_db()
	scrape_book_par(starting_url, num_processes=5, num_books=30, dump_to_json=False, dump_to_db=True, books_collection=books_collection)
	scrape_author_par(num_processes=5, num_authors=5, dump_to_json=False, dump_to_db=True, authors_collection=authors_collection)



def test_scrape_mult_both(starting_url):
	"""Test scraping multiple authors and books to dump to both mongodb and json file"""
	books_collection, authors_collection = connect_to_db()
	scrape_book_par(starting_url, num_processes=5, num_books=30, dump_to_json=True, dump_to_db=True, books_collection=books_collection)
	scrape_author_par(num_processes=5, num_authors=5, dump_to_json=True, dump_to_db=True, authors_collection=authors_collection)



def test_full(starting_url):
	"""Test all functionalities"""
	books_collection, authors_collection = connect_to_db()
	scrape_book_par(starting_url, num_processes=5, num_books=200, dump_to_json=True, dump_to_db=True, books_collection=books_collection)
	scrape_author_par(num_processes=5, num_authors=50, dump_to_json=True, dump_to_db=True, authors_collection=authors_collection)	


def main():
	#test_scrape_one_json(STARTING_PAGE)
	#test_scrape_one_db(STARTING_PAGE)
	#test_scrape_one_both(STARTING_PAGE)
	#test_scrape_mult_json(STARTING_PAGE)
	#test_scrape_mult_db(STARTING_PAGE)
	test_scrape_mult_both(STARTING_PAGE)
	#test_full(STARTING_PAGE)


if __name__ == "__main__":
	main()