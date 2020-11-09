# skiplist-elastic-plugin

Original implementation https://github.com/jrots/skiplist-elastic-plugin

Current version updated to work with elasticsearch 7.2.0

## Installation
(Only tested with elastic 7.2.0)
```
sudo bin/elasticsearch-plugin install https://github.com/mvSapphire/skiplist-elastic-plugin/blob/master/target/releases/searchFilter-plugin-7.2.0-SNAPSHOT.zip?raw=true
```
## Usage, sample call : 
```
....
curl -XGET 'localhost:9200/_search?pretty' -H 'Content-Type: application/json' -d'
{
    "query": {
        "function_score": {
	    "min_score" : 0.001,
            "query": {
               "match_all": {}
            },
            "script_score" : {
                "script" : {
                  "lang" : "skiplist",
                  "source" : "roaring",
                  "params" : {
                      "field" : "_id",
                      "data" : "OjAAAAEAAAAAAAYAEAAAAAEAAgADAAQABQBkAOgD"
                  }
                }
            }
        }
    }
}
```