# The purpose of this file is to document the steps taken to date.

######
SERVER
######
* server setup - shared hosting [unlimited SSD storage, unmetered bandwidth]
* ability to make MySQL databases (if necessary)
* custom local install of python version 3.12.9
* domain registered - www.OriginalCDCMirror.org
* domain privacy
* SSL Let's Encrypt Certificates applied to main domain and subdomain

########
PROGRESS
########
#. added subdomain "dev" to domain
#. created folder "public" for web served content
#. in the folder above public, created src where all coding will occur
#. in public, created "docs" for the sphinx documentation to go
#. pulled CDC directory from URL list found `here <https://github.com/end-of-term/eot2024/tree/98d5d13ac6bd115713c2cc1f37fa7db3012dd8e3/seed-lists>`_ 
	* the description: "CDC html URLs from sitemap data - 20241201.csv - file of about 46,000 .html URLs created by parsing the CDC's sitemap file at https://www.cdc.gov/wcms-auto-sitemap-index.xml, which then pointed to other sitemaps, which pointed to .html files."
	* this is CDC_html_URLs_From_sitemap_data_-_20241201.csv
#. used create_CDC_tree.py to create mirror directory structure
	* test_urls.csv was used to first test the code
	* create_cdc_structure.log is the log of that action
#. began writing code to retrieve all snapshots from URL list
	* snapshot had to have timestamp between 20241105 and 20250119
	* snapshot had to be closest to 20250119
	* retrieve_snapshot.py is where I got to this evening (2/17)
	* have not had a fully successful test yet
#. I have started processing codes as follows:
	* extraction_processing.py was my first crack at WARC extraction code
	* process_html.py was my first crack at a script that would accomplish the following:
		#. only retain the following types of tags in the <head> of the code:
			* any <title>
			* any <link that had a "rel" property equal to "stylesheet"
			* any <meta that had a "name" property equal to "cdc:last_published"
		#. replace text of any URL containing cdc.gov with OriginalCDCMirror.org
		#. strip the <header> tags and content from the page
