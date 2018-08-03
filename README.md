FTS3/4 is more mature. An instance list is stored as one record. Sometimes large b-trees are merged, slowing down mutating statements. FTS5 divides large instance lists across multiple records, and does incremental merging of b-trees.

https://www.sqlite.org/fts3.html
https://www.sqlite.org/fts5.html

## Carthage

`carthage update --platform Mac` toevoegen onder General, Embedded Libraries.

## Dropbox

