local driver = require "mongo.driver"
local bson = require "mongo.bson"
local empty_bson = bson.encode({})
local mongo = {}
local mongo_client = {}
local mongo_db = {}
local mongo_collection = {}
local mongo_cursor = {}

local client_meta = {
    __index = function(self, key)
        return rawget(mongo_client, key) or self:getDB(key)
    end,
    __tostring = function (self)
        local port_string
        if self.port then
            port_string = ":" .. tostring(self.port)
        else
            port_string = ""
        end

        return "[mongo client : " .. self.host .. port_string .."]"
    end,
}

local db_meta = {
    __index = function (self, key)
        return rawget(mongo_db, key) or self:getCollection(key)
    end,
    __tostring = function (self)
        return "[mongo db : " .. self.name .. "]"
    end
}

local collection_meta = {
    __index = function(self, key)
        return rawget(mongo_collection, key)
    end ,
    __tostring = function (self)
        return "[mongo collection : " .. self.full_name .. "]"
    end
}

local cursor_meta = {
    __index = mongo_cursor,
}

local function split(hostport)
    local host, port = driver.split(hostport)
    return host, port or 27017
end
------------------------Making a Connection--------------------
function mongo.client(conf)
    assert(conf.host)
    local first = conf
    local host, port = split(conf.host)
    local client = {
        host = host,
        port = port,
        username = first.username,
        password = first.password,
    }
    assert(client.host and client.port)
    local protocol = "mongodb://"
    local userinfo = ""
    if client.username and client.password then
        userinfo = string.format("%s:%s@", client.usernamei, client.password)
    end
    local hostinfo = string.format("%s:%d/", host, port)

    local uri = string.format("%s%s%s", protocol, userinfo, hostinfo)
    client.__client = driver.new(uri)
    setmetatable(client, client_meta)
    return client
end

function mongo_client:getDB(dbname)
    local db = {
        name = dbname,
        full_name = dbname,
    }
    db.__db = driver.db(self.__client, dbname)
    self[dbname] = setmetatable(db, db_meta)
    return db
end

function mongo_db:getCollection(collection)
    local col = {
        name = collection,
        full_name = self.full_name .. "." .. collection,
    }
    col.__collection = driver.coll(self.__db, collection)
    self[collection] = setmetatable(col, collection_meta)
    return col
end

------------------------Basic Operations------------------------
function mongo_collection:count(query)
    return driver.count(self.__collection, query and bson.encode(query) or empty_bson)
end

function mongo_collection:insert(doc)
    return driver.insert(self.__collection, bson.encode(doc))
end

function mongo_collection:delete(query)
    return driver.delete(self.__collection, bson.encode(query))
end

function mongo_collection:update(query, doc)
    return driver.update(self.__collection, bson.encode(query), bson.encode(doc))
end

function mongo_collection:find(query, fields)
    local cursor = {
        __collection = self.__collection,
        __query = query and bson.encode(query) or empty_bson,
        __fields = fields and bson.encode(fields),
        __sortquery = nil,
        __cursor = nil,
        __skip = 0,
        __limit = 0,
    }
    setmetatable(cursor, cursor_meta)
    return cursor
end

function mongo_collection:findOne(query, fields)
    local cursor = driver.find(self.__collection, 
            query and bson.encode(query) or empty_bson, 
            0, 1, fields and bson.encode(fields))
    local doc = driver.next(cursor)
    return doc
end

--findAndModify(query, sort, update, fields, remove, upsert, new)
--@query    table
--@sort     table
--@update   table 
--@fields   table
--@remove   boolean
--@upsert   boolean
--@new      boolean
function mongo_collection:findAndModify(doc)
    local query  = doc.query
    local sort   = doc.sort
    local update = doc.update
    local fields = doc.fields
    assert(query)
    assert(update or remove)
    local ret = driver.findAndModify(self.__collection, 
            bson.encode(query), 
            sort and bson.encode(sort), 
            update and bson.encode(update), 
            fields and bson.encode(fields), 
            doc.remove, doc.upsert, doc.new)
    assert(ret.ok == 1)
    return ret.value
end

function mongo_cursor:hasNext()
    if not self.__cursor then
        self.__cursor = driver.find(self.__collection, self.__sortquery or self.__query, self.__skip, self.__limit, self.__fields)
    end
    local doc = driver.next(self.__cursor)
    if doc then
        self.__document = doc
        return true
    end
    return false
end

function mongo_cursor:next()
    assert(self.__document, "please call hasNext first.")
    return self.__document
end

function mongo_cursor:skip(amount)
    self.__skip = amount
    return self
end

function mongo_cursor:limit(amount)
    self.__limit = amount
    return self
end

--设置排序字段
--@...  参数列表
--e.g: "field", 1, "field2", -1
function mongo_cursor:sort(...)
    self.__sortquery = bson.encode({["$query"] = self.__query, 
            ["$orderby"] = bson.encode_order(...)})
    return self
end

return mongo
