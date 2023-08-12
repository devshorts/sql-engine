# sql-engine
A toy sql engine in go. Reads in newline delimited JSON and allows you to write simple sql to query against it. 


# example

```
$ ./out/sql "select foo" < test/sample.dat
{"foo":"1"}
{"foo":"1"}
{"foo":"3"}
```

```
$ ./out/sql "select *" < test/sample.dat
{"foo":"1"}
{"bar":"2","foo":"1"}
{"bar":"3","foo":"3"}
```
