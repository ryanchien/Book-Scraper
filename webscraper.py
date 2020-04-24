import json
import os
import time
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

def new_sel_driver():
	"""Initialize selenium driver and adds adblocking extension to optimize performance"""
	options = Options()
	options.add_extension(os.getcwd() + "/" + "extension_1_24_4_0.crx")
	path_to_driver = os.getcwd() + "/" + "chromedriver"
	driver = webdriver.Chrome(executable_path=path_to_driver, chrome_options=options)
	return driver


def insert_elem_text_to_dict(driver, dictionary, json_key, elem_id="", elem_class_name="", elem_xpath=""):
	"""Inserts specific element attributes into given dictionary based on specific element type"""
	elem = None
	if elem_id != "":
		try:
			elem = driver.find_element_by_id(elem_id)
		except NoSuchElementException:
			elem = None
	elif elem_class_name != "":
		try:
			elem = driver.find_element_by_class_name(elem_class_name)
		except NoSuchElementException:
			elem = None
	elif elem_xpath != "":
		try:
			elem = driver.find_element_by_xpath(elem_xpath)
		except NoSuchElementException:
			elem = None
	if elem == None:
		dictionary[json_key] = None
		return
	if json_key == "authors":
		dictionary[json_key] = elem.text[3:].split(", ")
	elif json_key == "rating":
		dictionary[json_key] = float(elem.text)
	elif json_key == "rating_count" or json_key == "review_count":
		dictionary[json_key] = [int(char) for char in elem.text.replace(',', '').split() if char.isdigit()][0]
	elif json_key == "image_url":
		dictionary[json_key] = elem.get_attribute("src")
	else:
		dictionary[json_key] = elem.text


def parse_unique_id_from_url(page_url):
	"""Parse unique id number from url for both author and book"""
	tokens = page_url.split("/")
	last_token = tokens[len(tokens)-1]
	book_id = ""
	for char in last_token:
		if char.isdigit():
			book_id += char
		else:
			break
	return int(book_id)

def get_similar_books(driver, dictionary):
	"""Finds the see similar books... link and returns books in that list"""
	next_books_to_scrape = []
	similar_books = []
	WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.LINK_TEXT, "See similar books…"))).click()
	similar_elem_1 = driver.find_element_by_xpath("/html/body/div[4]/div/div[3]/div[1]/div[3]/div/div/div[1]/div/div/div[2]/a/span")
	parent_elem_1 = driver.find_element_by_xpath("/html/body/div[4]/div/div[3]/div[1]/div[3]/div/div/div[1]/div/div/div[2]/a")
	similar_elem_2 = driver.find_element_by_xpath("/html/body/div[4]/div/div[3]/div[1]/div[3]/div/div/div[2]/div/div/div[2]/a/span")
	parent_elem_2 = driver.find_element_by_xpath("/html/body/div[4]/div/div[3]/div[1]/div[3]/div/div/div[2]/div/div/div[2]/a")
	similar_books.append(similar_elem_1.text)
	similar_books.append(similar_elem_2.text)
	next_books_to_scrape.append(parent_elem_1.get_attribute('href'))
	next_books_to_scrape.append(parent_elem_2.get_attribute('href'))
	for i in range(1,50):
		try:
			similar_elem = driver.find_element_by_xpath("/html/body/div[4]/div/div[3]/div[1]/div[5]/div/div[" + str(i) +"]/div/div/div[2]/a/span")
			parent_elem = driver.find_element_by_xpath("/html/body/div[4]/div/div[3]/div[1]/div[5]/div/div[" + str(i) +"]/div/div/div[2]/a")
		except NoSuchElementException:
			break
		similar_books.append(similar_elem.text)
		next_books_to_scrape.append(parent_elem.get_attribute('href'))
	dictionary["similar_books"] = similar_books
	return next_books_to_scrape


def get_book_author_urls(driver, dictionary):
	"""Finds all the author urls for a given book"""
	author_urls = []
	for i in range(1, len(dictionary["authors"])+1):
		author_elem = driver.find_element_by_xpath("//*[@id=\"bookAuthors\"]/span[2]/div[" + str(i) +"]/a")
		author_urls.append(author_elem.get_attribute('href'))
	dictionary["author_urls"] = author_urls


def get_author_image_url(driver, dictionary):
	"""Finds the author image for a given author"""
	elem_url = None
	try:
		elem_url = driver.find_element_by_xpath("/html/body/div[2]/div[3]/div[1]/div[2]/div[3]/div[1]/a/img").get_attribute("src")
	except NoSuchElementException:
		elem_url = "No Image Found"
	dictionary["image_url"] = elem_url


def get_similar_authors(driver, dictionary):
	"""Finds the similar authors link and returns authors in that list"""
	similar_authors = []
	try:
		WebDriverWait(driver, 1).until(EC.element_to_be_clickable((By.LINK_TEXT, "Similar authors"))).click()
	except TimeoutException:
		dictionary["similar_authors"] = ["None"]
		return		
	similar_elem_1 = driver.find_element_by_xpath("/html/body/div[4]/div/div[3]/div[1]/div[3]/div/div/div[1]/div/div/div[2]/div[1]/a/span")
	similar_elem_2 = driver.find_element_by_xpath("/html/body/div[4]/div/div[3]/div[1]/div[3]/div/div/div[2]/div/div/div[2]/div[1]/a/span")
	similar_authors.append(similar_elem_1.text)
	similar_authors.append(similar_elem_2.text)
	for i in range(1,50):
		try:
			similar_elem = driver.find_element_by_xpath("/html/body/div[4]/div/div[3]/div[1]/div[5]/div/div[" + str(i) +"]/div/div/div[2]/div[1]/a/span")
		except NoSuchElementException:
			break
		similar_authors.append(similar_elem.text)
	dictionary["similar_authors"] = similar_authors


def get_author_books(page_url, driver, dictionary):
	"""Finds books written by a given author"""
	driver.get(page_url.replace("author/show/","author/list/") + "?per_page=300")
	author_books = []
	for i in range(1,300):
		try:
			book_elem = driver.find_element_by_xpath("/html/body/div[2]/div[3]/div[1]/div[1]/div[2]/table/tbody/tr[" + str(i) + "]/td[2]/a/span")
		except NoSuchElementException:
			break
		author_books.append(book_elem.text)
	dictionary["author_books"] = author_books


def get_book_data(page_url, driver, books_to_scrape):
	"""Wrapper function to find all book data and return as dict"""
	book_data = {"book_url": page_url, "_id": parse_unique_id_from_url(page_url)}
	driver.get(page_url)
	insert_elem_text_to_dict(driver, book_data, "title", elem_id="bookTitle")
	insert_elem_text_to_dict(driver, book_data, "authors", elem_id="bookAuthors")
	insert_elem_text_to_dict(driver, book_data, "rating", elem_xpath="//*[@id=\"bookMeta\"]/span[2]")
	insert_elem_text_to_dict(driver, book_data, "rating_count", elem_xpath="//*[@id=\"bookMeta\"]/a[2]")
	insert_elem_text_to_dict(driver, book_data, "review_count", elem_xpath="//*[@id=\"bookMeta\"]/a[3]")
	insert_elem_text_to_dict(driver, book_data, "image_url", elem_xpath="//*[@id=\"coverImage\"]")
	insert_elem_text_to_dict(driver, book_data, "ISBN", elem_xpath="//*[@id=\"bookDataBox\"]/div[2]/div[2]")	
	get_book_author_urls(driver, book_data)
	books_to_scrape.append(get_similar_books(driver, book_data))
	driver.close()
	return book_data


def get_author_data(page_url, driver):
	"""Wrapper function to find all author data and return as dict"""
	author_data = {"author_url": page_url, "_id": parse_unique_id_from_url(page_url)}
	driver.get(page_url)
	insert_elem_text_to_dict(driver, author_data, "name", elem_xpath="/html/body/div[2]/div[3]/div[1]/div[2]/div[3]/div[2]/div[2]/h1/span")
	insert_elem_text_to_dict(driver, author_data, "rating", elem_class_name="average")
	insert_elem_text_to_dict(driver, author_data, "rating_count", elem_class_name="votes")
	insert_elem_text_to_dict(driver, author_data, "review_count", elem_class_name="count")
	get_author_image_url(driver, author_data)
	get_similar_authors(driver, author_data)
	get_author_books(page_url, driver, author_data)
	driver.close()
	return author_data