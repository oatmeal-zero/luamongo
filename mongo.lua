--local json = require "json"
local driver = require "mongo.driver"
local empty_table = {}
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

------------------------Making a Connection--------------------
function mongo.client(conf)
    local first = conf
    local client = {
        host = first.host,
        port = first.port or 27017,
        username = first.username,
        password = first.password,
    }
    assert(client.host and client.port)
    local uri = string.format("mongodb://%s:%d/", 
            client.host, client.port)
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
    return driver.count(self.__collection, query or empty_table)
end

function mongo_collection:insert(doc)
    return driver.insert(self.__collection, doc)
end

function mongo_collection:delete(query)
    return driver.delete(self.__collection, query)
end

function mongo_collection:update(query, doc)
    return driver.update(self.__collection, query, doc)
end

function mongo_collection:find(query, fields)
    local cursor = {
        __collection = self.__collection,
        __query = query or empty_table,
        __fields = fields,
        __sortquery = nil,
        __cursor = nil,
        __skip = 0,
        __limit = 0,
    }
    setmetatable(cursor, cursor_meta)
    return cursor
end

function mongo_collection:findOne(query, fields)
    local cursor = driver.find(self.__collection, query or empty_table, 0, 1, fields)
    local doc = driver.next(cursor)
    driver.kill(cursor)
    return doc
end

--findAndModify(query, sort, update, fields, remove, upsert, new)
--@query    json
--@sort     json
--@update   json
--@fields   json
--@remove   boolean
--@upsert   boolean
--@new      boolean
function mongo_collection:findAndModify(doc)
    assert(doc.query)
    assert(doc.update or doc.remove)
    return driver.findAndModify(self.__collection, doc.query, doc.sort, 
            doc.update, doc.fields, doc.remove, doc.upsert, doc.new)
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
    driver.kill(self.__cursor)
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

function mongo_cursor:sort(sort)
    self.__sortquery = {["$query"] = self.__query, ["$orderby"] = sort}
    return self
end

return mongo
