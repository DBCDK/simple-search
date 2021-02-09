# Simple Search

Search engine prototype for Randers library

The prototype consists of a solr with records relevant for Randers, a search-engine and a gui for testing.

## Solr

The records in the solr are part of the Randers collection + material from ereolen and filsmstriben.
We use data from holdings as well as collection-identifiers to find and harvest all relevant documents.

From the original records work documents are constructed and indexed into the simple-search solr.
That means that the simple-search documents are on the work level and each document can cover several pids.

Several fields are constructed from the original *creator*, *title*
and *subject* fields. Furthermore *language*, *type* and
*collection-identifiers* are harvested, so they can be part of the
services response.

The schema for the solr can be found [here](https://gitlab.dbc.dk/ai/simple-search-solr/-/blob/master/conf/conf/managed-schema)
in the [simple-search-solr project](https://gitlab.dbc.dk/ai/simple-search-solr)

## Search Engine

The search engeine is exposed through kubernetes at [http://simple-search-1-0.mi-prod.svc.cloud.dbc.dk/search](http://simple-search-1-0.mi-prod.svc.cloud.dbc.dk/search)

The search-engine is exposed as a **get** and **post** method. It has two parameters:
* **q**: query to base search on
* **debug**: If true debug information is present in response 

The result consist of a list of ranked results of works, where each item contains data for that particular work.

example of work item:

    {
       "language" : [
          "dan"
       ],
       "pid_details" : [
          {
             "pid" : "870970-basis:51701763",
             "type" : "Bog"
          },
          {
             "pid" : "870970-basis:51796527",
             "type" : "Ebog"
          }
       ],
       "pids" : [
          "870970-basis:51701763",
          "870970-basis:51796527"
       ],
       "debug" : {
          "work_type" : [
             "literature"
          ],
          "workid" : "work:1479116",
          "creator" : [
             "Mette E. Neerlin"
          ]
       },
       "title" : "Hest, hest, tiger, tiger"
    }

example of using post:

    curl -P -v -H "Content-Type: Application/json" -d '{"q": "hest", "debug": true}' "http://simple-search-1-0.mi-prod.svc.cloud.dbc.dk/search"

example of using get:

    http://simple-search-1-0.mi-prod.svc.cloud.dbc.dk/search?q=hest&debug=true

## Search GUI

The also provides a simple GUI for exploratory work. Each hit has a cover (if any) and links to [bibliotek.dk](https://bibliotek.dk/)

The GUI can be found at [http://simple-search-1-0.mi-prod.svc.cloud.dbc.dk](http://simple-search-1-0.mi-prod.svc.cloud.dbc.dk)

## Work-presentation data

The file `indexer_work_presentation.py` (at the time of writing) is a first attempt to do simple search with data from the work-presentation database. Here is an example of how to run it.

    WORK_PRESENTATION_URL=postgres://$WORK_PRESENTATION_POSTGRES_URL python src/simple_search/solr/indexer_work_presentation.py 773000.pids http://localhost:8983/solr/simple-search/ work_to_holdings.joblib popularity-2018-2020.count.gz

where the `773000.pids` and the other files were found on artifactory `ai-generic/simple-search/`.
