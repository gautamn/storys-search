from pymongo import MongoClient
import re, os
import pysolr
import traceback
from dotenv import load_dotenv
import datetime

def current_time():
    return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Create a connection to Solr
load_dotenv()
solr_endpoint=os.getenv("SOLR_STORYPAGES_ENDPOINT")
solr = pysolr.Solr(solr_endpoint, timeout=10)
print(f"{current_time()} - All initialisations done!")

def remove_non_unicode(text) -> str:
    # Define a regular expression pattern to match non-Unicode characters
    pattern = re.compile(r'[^\x00-\x7F]+')
    # Use the pattern to replace non-Unicode characters with an empty string
    cleaned_text = pattern.sub('', text)
    return cleaned_text

def index_documents(solr_docs) -> None:
	solr.add(solr_docs)
	# Commit the changes to make them visible in the index
	solr.commit()
 
def delete_documents() -> None: 
	# Delete all documents by querying all documents (*:*)
	solr.delete(q='*:*')
	# Commit the changes to make the deletion permanent
	solr.commit() 

def fetch_lightx_storys():
	try:
		mongodb_host=os.getenv("MONGODB_HOST")		
		client = MongoClient(mongodb_host)
		mongodb_name=os.getenv("MONGODB_NAME")
		print(f"{current_time()} - trying to connect mongodb database={mongodb_name}")
		db = client[mongodb_name]
		query = {
			"appname": "lightx",
			"pageType": "story",
			"status": "complete",
			"language": "en"
		}	
		story_pages=db["storyPage"]
		#story_pages=db["storys"]
		result = story_pages.find(query)
		return result
	except Exception as e:
		print(e)
		print(f"{current_time()} - Error occured while fetching story pages from MongoDB.")
		traceback.format_exc()

def parse_storys(result) -> list:
	solr_docs=[]
	index=1
	for doc in result:
		if doc == None:
			return
		#print(f"parsing solr document with index={index}")
		index = index + 1
		solr_doc={}
		page_title=doc['pageTitle']
		page_description=doc['pageDescription']
		page_title=remove_non_unicode(page_title)
		page_description=remove_non_unicode(page_description)
		solr_doc['page_title_t']=page_title
		solr_doc['page_description_t']=page_description
  
		_id_value =doc.get('_id')
		solr_doc['id']=str(_id_value)
        #solr_doc['id']=index
		solr_doc['story_type_s']=doc['type']
		solr_doc['product_id_s']=doc['productId']
		solr_doc['page_title_t']=page_title
		solr_doc['page_description_t']=page_description
		solr_doc['type_s']=doc['type']
		solr_doc['pageType_s']=doc['pageType']
		solr_doc['subType_s']=doc['subType']

		tool_tag_assoc_ids = []
		if 'toolTags' in doc:
			tool_tags = doc['toolTags']
			for tool_tag in tool_tags:
				tools = tool_tag['tools']
				for tool in tools:
					tool_tag_assoc_id = tool['toolTagAssocId']
					tool_tag_assoc_ids.append(tool_tag_assoc_id)

			print(f'{current_time()} - tool_tag_assoc_ids={tool_tag_assoc_ids}')
			unique_list = list(set(tool_tag_assoc_ids))
			solr_doc['tool_tag_assoc_ss']=unique_list
  

		first_section=doc["sections"][0]
		if 'title' in first_section: 
			first_section_title=first_section['title']

		if 'desc1' in first_section:
			first_section_desc=first_section['desc1']	

		solr_doc['first_section_title_t']=first_section_title
		solr_doc['first_section_desc_t']=first_section_desc		

		sections=doc["sections"]
		content=""
		for section in sections:
			section_type=section['type']
			if section_type=='banner':
				section_title=section['title']
				section_desc_1=section['desc1']
				solr_doc['banner_title_t']=section_title
				solr_doc['banner_description_t']=section_desc_1
				
			if section_type=='steps':
				steps = section['steps']
				temp_str = ''
				for step in steps:
					step_title=step['title']
					step_desc=step['desc']
					temp_str = step_title + "\n" + step_desc + "\n\n"
				solr_doc['steps_t']=remove_non_unicode(temp_str)
				
			if section_type=='questions':
				qnas = section['qa']
				temp_str = ''
				for qna in qnas:
					question=qna['que']
					answer=qna['ans']
					temp_str = question + "\n" + answer + "\n\n"
				solr_doc['questions_t']=remove_non_unicode(temp_str)

			if section_type=='section':
				section_title=section['title']
				section_desc=section['desc1']
				content = content + "\n" + section_title + "\n" + section_desc + "\n\n"	

		content=remove_non_unicode(content)
		solr_doc['section_t']=remove_non_unicode(content)  
		#print(f"solr_doc={solr_doc}")
		#print("**********************************************************************************************")
		solr_docs.append(solr_doc)
	return solr_docs

if __name__ == "__main__":
    result=fetch_lightx_storys()
    solr_docs=parse_storys(result)
    print(f"{current_time()} - Total number of documents to be indexed={len(solr_docs)}")
    delete_documents()
    print(f"{current_time()} - Deleted all indexed documents.")
    index_documents(solr_docs)
    print(f"{current_time()} - Indexing completed! Total documents indexed={len(solr_docs)}")
