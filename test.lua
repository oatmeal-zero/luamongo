local mongo = require "mongo"

local client = mongo.client({host = "192.168.1.45"})
print("client:", client)
local db = client.player
print("db:", db)
local collection = db.playerset
print("collection:", collection)
local collection2 = db.playerset
print("collection2:", collection2)
--注意：json中是使用双引号的!!
local count = db.playerset:count({_id = 2048})
print("count:", count)
collectgarbage("collect")
local count = db.playerset:count()
print("count:", count)

local dbt = client.test
--dbt.testcoll:insert({"lwx", "lwx123456"})
--dbt.testcoll:insert({name = "lwx", passwd = "lwx123456", tbl = {hello = "world"}, array = {123,456,789}})
--dbt.testcoll:insert({[1] = "lwx", [2] = "lwx123456", [23] = 123})
--dbt.testcoll:insert('{"name": "lwx", "passwd": "lwx123456"}')
--dbt.testcoll:update('{"name": "lwx"}', '{"$set": {"passwd2": 123}}')
--dbt.testcoll:delete('{"name": "lwx"}')

function printtale(t)
    print("==>>", t, "<<==")
    for k,v in pairs(t) do
        print(type(k),k,v)
        if type(v) == "table" then
            printtale(v)
        end
    end
    print("-------", t, "end.-------")
end

--local cursor = db.cheat:find():skip(2):limit(2):sort('{"score":-1}')
--local cursor = db.cheat:find({playerId = 2048}):sort({score = 1})
local cursor = db.cheat:find({raceMode = 2}):sort("score", 1, "playerId", 1)
--local cursor = db.cheat:find({raceMode = 2}):sort("playerId", 1, "score", 1)
while cursor:hasNext() do
    local doc = cursor:next()
    --printtale(doc)
end
print("--------------------findOne-----------------------------")
local doc = db.cheat:findOne({mapId= 1, playerId= 18254, })
--printtale(doc)
collectgarbage("collect")

dbt.testcoll:insert({pkey=1111,double= 3.1415926, utf8= "你好", document={key1= 123, key2= "abc"},array={111,222,333},bool=false,bool2=true,int32=12345678, int64=34359738368,date_time=0,timestamp=0})
print("-------------------------------------------------")
local doc = dbt.testcoll:findOne({pkey = 1111})
--printtale(doc)

print("-------------------findAndModify----------------------")
local dbg = client.global
local doc = dbg.counters:findAndModify({query = {_id = "testid"},
        update = {["$inc"] = {seq = 1}},
        upsert = true,
        new = true})
--printtale(doc)
local seq = doc.value.seq
print(type(seq), seq)
