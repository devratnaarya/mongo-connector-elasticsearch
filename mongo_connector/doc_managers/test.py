from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27018")
m_product = client.catalog.product
m_variant = client.catalog.variant
m_brand = client.catalog.brand
m_brand_d = client.catalog.brand_distribution

count = 1
for doc in m_product.find({"brand_name" : None}):
	brand_dis = m_brand_d.find_one({'name' : doc['brand_distribution']})
	if brand_dis:
		brand = m_brand.find_one({"_id" : brand_dis['brand_id']})
		if brand:
			updateObj = {
				"brand_name" : brand['name']
			}

			result = m_product.update_one({'_id':doc['_id']}, {"$set": updateObj}, upsert=False)
			print "count : "+str(count)
			count = count+1
