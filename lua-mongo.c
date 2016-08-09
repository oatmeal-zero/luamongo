#include <lua.h>
#include <lauxlib.h>
#include <mongoc.h>

#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

extern void 
bson_as_table(lua_State *L, const bson_t *bson);
bson_t *
table_as_bson(lua_State *L, int index);

static int
op_new(lua_State *L) {
    mongoc_client_t *client;
    const char *uri;

    mongoc_init();
    uri = luaL_checkstring(L, 1);
    client = mongoc_client_new (uri);
    lua_pushlightuserdata(L, client);
    return 1;
}

static int
op_db(lua_State *L) {
    mongoc_database_t *database;
    mongoc_client_t *client;
    const char *name;

    client = lua_touserdata(L, 1);
    name = luaL_checkstring(L, 2);
    database = mongoc_client_get_database(client, name);
    lua_pushlightuserdata(L, database);
    return 1;
}

static int
op_coll(lua_State *L) {
    mongoc_database_t *database;
    mongoc_collection_t *collection;
    const char *name;

    database = lua_touserdata(L, 1);
    name = luaL_checkstring(L, 2);
    collection = mongoc_database_get_collection(database, name);
    lua_pushlightuserdata(L, collection);
    return 1;
}

static int
op_count(lua_State *L) {
    mongoc_collection_t *collection;
    bson_error_t error;
    bson_t *doc;
    int64_t count;

    collection = lua_touserdata(L, 1);
    doc = table_as_bson (L, 2);
    if (!doc) {
        luaL_error(L, "can't convert to bson from table.");
    }

    count = mongoc_collection_count (collection, MONGOC_QUERY_NONE, doc, 0, 0, NULL, &error);
    if (count < 0) {
        bson_destroy (doc);
        luaL_error (L, error.message);
    } 

    bson_destroy (doc);
    lua_pushinteger (L, count);
    return 1;
}

static int
op_insert(lua_State *L) {
    mongoc_collection_t *collection;
    bson_error_t error;
    bson_t *doc;

    collection = lua_touserdata(L, 1);
    doc = table_as_bson (L, 2);
    if (!doc) {
        luaL_error(L, "can't convert to bson from table.");
    }

    if (!mongoc_collection_insert (collection, MONGOC_INSERT_NONE, doc, NULL, &error)) {
        bson_destroy (doc);
        luaL_error (L, error.message);
    }

    bson_destroy (doc);
    return 0;
}

static int
op_delete(lua_State *L) {
    mongoc_collection_t *collection;
    bson_error_t error;
    bson_t *doc;

    collection = lua_touserdata(L, 1);
    doc = table_as_bson (L, 2);
    if (!doc) {
        luaL_error(L, "can't convert to bson from table.");
    }   

    if (!mongoc_collection_remove (collection, MONGOC_REMOVE_NONE, doc, NULL, &error)) {
        bson_destroy (doc);
        luaL_error (L, error.message);
    }

    bson_destroy (doc);
    return 0;
}

static int
op_update(lua_State *L) {
    mongoc_collection_t *collection;
    bson_error_t error;
    bson_t *query;
    bson_t *update;

    collection = lua_touserdata(L, 1);
    query = table_as_bson (L, 2);
    if (!query) {
        luaL_error(L, "can't convert to bson from table.");
    }   

    update = table_as_bson (L, 3);
    if (!update) {
        bson_destroy (query);
        luaL_error(L, "can't convert to bson from table.");
    }   

    if (!mongoc_collection_update (collection, MONGOC_UPDATE_MULTI_UPDATE, query, update, NULL, &error)) {
        bson_destroy (query);
        bson_destroy (update);
        luaL_error (L, error.message);
    }

    bson_destroy (query);
    bson_destroy (update);
    return 0;
}

static int
op_find(lua_State *L) {
    mongoc_cursor_t *cursor;
    bson_error_t error;
    mongoc_collection_t *collection;
    bson_t *query;
    bson_t *fields;
    uint32_t skip;
    uint32_t limit;

    collection = lua_touserdata(L, 1);
    query = table_as_bson (L, 2);
    if (!query) {
        luaL_error(L, "can't convert to bson from table.");
    }   

    skip = luaL_checkinteger (L, 3);
    limit = luaL_checkinteger(L, 4);

    // fields optional
    fields = NULL;
    if (lua_istable(L, 5)) {
        fields = table_as_bson (L, 5);
        if (!fields) {
            bson_destroy (query);
            luaL_error(L, "can't convert to bson from table.");
        }   
    }

    cursor = mongoc_collection_find (collection, MONGOC_QUERY_NONE, skip, limit, 0, query, fields, NULL);
    lua_pushlightuserdata(L, cursor);
    bson_destroy (query);
    if (fields) bson_destroy(fields);
    return 1;
}

static int 
op_next(lua_State *L) {
    mongoc_cursor_t *cursor;
    const bson_t *doc;

    cursor = lua_touserdata(L, 1);
    if (!mongoc_cursor_next (cursor, &doc)) {
        return 0;
    }

    bson_as_table(L, doc);
    return 1;
}

static int
op_kill(lua_State *L) {
    mongoc_cursor_t *cursor;
    cursor = lua_touserdata(L, 1);
    mongoc_cursor_destroy (cursor);
    return 0;
}

static int
op_find_and_modify(lua_State *L) {
    mongoc_collection_t *collection;
    bson_error_t error;
    bson_t *query=NULL;
    bson_t *sort=NULL;
    bson_t *update=NULL;
    bson_t *fields=NULL;
    bson_t reply;
    bool remove;
    bool upsert;
    bool new;
    const char* json;

    collection = lua_touserdata(L, 1);
    query = table_as_bson (L, 2); 
    if (!query) {
        luaL_error(L, "can't convert to bson from table.");
    }   

    // optional
    if (lua_istable(L, 3)) {
        sort = table_as_bson (L, 3); 
        if (!sort) {
            luaL_error(L, "can't convert to bson from table.");
        }   
    }

    // optional
    if (lua_istable(L, 4)) {
        update = table_as_bson (L, 4); 
        if (!update) {
            luaL_error(L, "can't convert to bson from table.");
        }   
    }

    // optional
    if (lua_istable(L, 5)) {
        fields = table_as_bson (L, 5); 
        if (!fields) {
            luaL_error(L, "can't convert to bson from table.");
        }   
    }

    remove = lua_toboolean(L, 6);
    upsert = lua_toboolean(L, 7);
    new = lua_toboolean(L, 8);

    if (!mongoc_collection_find_and_modify (collection, query, sort, update, fields, remove, upsert, new, &reply, &error)) {
        if (query) bson_destroy(query);
        if (sort) bson_destroy(sort);
        if (update) bson_destroy(update);
        if (fields) bson_destroy(fields);
        luaL_error (L, error.message);
    }

    if (query) bson_destroy(query);
    if (sort) bson_destroy(sort);
    if (update) bson_destroy(update);
    if (fields) bson_destroy(fields);

    bson_as_table(L, &reply);
    return 1;
}

int
luaopen_mongo_driver(lua_State *L) {
    //luaL_checkversion(L);
    luaL_Reg l[] ={
        { "new",    op_new },
        { "db",     op_db },
        { "coll",   op_coll },
        { "count",  op_count },
        { "insert", op_insert },
        { "delete", op_delete },
        { "update", op_update },
        { "find",   op_find },
        { "next",   op_next },
        { "kill",   op_kill },
        { "findAndModify",   op_find_and_modify },
        { NULL,     NULL },
    };

    luaL_newlib(L,l);
    return 1;
}
