## wut
schemaless postgres with esoteric http api

## requirements
postgres with [jsquery](https://github.com/postgrespro/jsquery) and [jsonb](http://www.postgresql.org/docs/9.4/static/datatype-json.html) is required

## usage
```
http 127.0.0.1:8080/fetch query:=[1,2,3]
http 127.0.0.1:8080/fetch query:=[1] depth:=3 'follow:=["child"]'
http 127.0.0.1:8080/persist spam:=1
http 127.0.0.1:8080/fetch query='spam = 1' depth:=2 'follow:=["spam"]'
```
