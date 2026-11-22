create database PhotoJournal
use PhotoJournal
create bucket Post { _id_ uuid auto, Caption string, Photos blobStore, Comments fifoStore }
db.Post.insert({_id_: "p1", Caption: "sunrise"})
db.Post.insert({_id_: "p2", Caption: "sunset"})
db.Post["p1"].Comments.push({Author: "ada", Text: "first"})
db.Post["p1"].Comments.push({Author: "bob", Text: "second"})
db.Post["p1"].Comments.push({Author: "cy", Text: "third"})
db.Post["p2"].Comments.push({Author: "dee", Text: "hello"})
db.Post["p1"].Comments.pop()
db.Post["p1"].Comments.len()
db.Post["p2"].Comments.len()
describe bucket Post
