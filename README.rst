To start this mongo-connector for syncing mongodb and elastic search 

make sure elasticsearch version installed above 2.x

run below command in different terminals:

mongo-connector -m localhost:27017 -t localhost:9200 -d elastic2_doc_variant -n catalog.variant -vvvv

mongo-connector -m localhost:27017 -t localhost:9200 -d elastic2_doc_facility_variant -n catalog.facility_variant -vvvv