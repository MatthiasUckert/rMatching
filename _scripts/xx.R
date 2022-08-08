library(superml)

docs <- table_target$name

sentence <- table_source$name[1]


s <- bm_25(document = sentence, corpus = docs, top_n = 2)
s[max(s)]



docs[which.max(stringdist::stringsim(sentence, docs))]



