#include <lua.h>
#include <lauxlib.h>
#include <mongoc.h>

#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

typedef struct
{
    int      array;
    lua_State *L;
} bson_lua_state_t;

typedef struct
{
    bson_t *bson;
} bson_wrap_t;

/*
 * Forward declarations.
 */
static bool _bson_as_table_visit_array (const bson_iter_t *iter,
        const char        *key,
        const bson_t      *v_array,
        void              *data);

static bool _bson_as_table_visit_document (const bson_iter_t *iter,
        const char        *key,
        const bson_t      *v_document,
        void              *data);

static bson_t *
_table_as_bson(lua_State *L, int index, bool array);

static inline bool
is_32bit(int64_t v) {
    return v >= INT32_MIN && v <= INT32_MAX;
}

static void
_bson_as_table_push_key (const char *key, void * data)
{
    bson_lua_state_t *state = data;
    lua_State *L = state->L;
    if (state->array > 0) {
        lua_pushinteger(L, state->array++);
    } else {
        lua_pushstring(L, key);
    }
}

static bool
_bson_as_table_visit_double (const bson_iter_t *iter,
        const char        *key,
        double             v_double,
        void              *data)
{
    bson_lua_state_t *state = data;
    lua_State *L = state->L;
    _bson_as_table_push_key(key, data);
    lua_pushnumber(L, v_double);
    lua_rawset(L, -3);
    return false;
}

static bool
_bson_as_table_visit_utf8 (const bson_iter_t *iter,
        const char        *key,
        size_t             v_utf8_len,
        const char        *v_utf8,
        void              *data)
{
    bson_lua_state_t *state = data;
    lua_State *L = state->L;
    _bson_as_table_push_key(key, data);
    lua_pushlstring(L, v_utf8, v_utf8_len);
    lua_rawset(L, -3);
    return false;
}

static bool
_bson_as_table_visit_bool (const bson_iter_t *iter,
        const char        *key,
        bool        v_bool,
        void              *data)
{
    bson_lua_state_t *state = data;
    lua_State *L = state->L;
    _bson_as_table_push_key(key, data);
    lua_pushboolean(L, v_bool);
    lua_rawset(L, -3);
    return false;
}

static bool
_bson_as_table_visit_date_time (const bson_iter_t *iter,
        const char        *key,
        int64_t       msec_since_epoch,
        void              *data)
{
    bson_lua_state_t *state = data;
    lua_State *L = state->L;
    _bson_as_table_push_key(key, data);
    lua_pushinteger(L, msec_since_epoch/1000);
    lua_rawset(L, -3);
    return false;
}

static bool
_bson_as_table_visit_timestamp (const bson_iter_t *iter,
        const char        *key,
        uint32_t      v_timestamp,
        uint32_t      v_increment,
        void              *data)
{
    bson_lua_state_t *state = data;
    lua_State *L = state->L;
    _bson_as_table_push_key(key, data);
    // TODO: not support now.
    //printf("key:%s val:%ld %ld\n", key, v_timestamp, v_increment);
    lua_pushinteger(L, 0);
    lua_rawset(L, -3);
    return false;
}

static bool
_bson_as_table_visit_oid (const bson_iter_t *iter,
        const char        *key,
        const bson_oid_t  *oid,
        void              *data)
{
    bson_lua_state_t *state = data;
    lua_State *L = state->L;
    char str[25];
    bson_oid_to_string (oid, str);
    _bson_as_table_push_key(key, data);
    //lua_pushstring(L, str);
    luaL_Buffer b;
    luaL_buffinit(L, &b);
    luaL_addchar(&b, 0);
    luaL_addchar(&b, BSON_TYPE_OID);
    luaL_addstring(&b, str);
    luaL_pushresult(&b);

    lua_rawset(L, -3);
    return false;
}

static bool
_bson_as_table_visit_int32 (const bson_iter_t *iter,
        const char        *key,
        int32_t       v_int32,
        void              *data)
{
    bson_lua_state_t *state = data;
    lua_State *L = state->L;
    _bson_as_table_push_key(key, data);
    lua_pushinteger(L, v_int32);
    lua_rawset(L, -3);
    return false;
}


static bool
_bson_as_table_visit_int64 (const bson_iter_t *iter,
        const char        *key,
        int64_t       v_int64,
        void              *data)
{
    bson_lua_state_t *state = data;
    lua_State *L = state->L;
    _bson_as_table_push_key(key, data);
    lua_pushinteger(L, v_int64);
    lua_rawset(L, -3);
    return false;
}

static const bson_visitor_t bson_as_table_visitors = {
    NULL, /* visit_before */
    NULL, /* visit_after */
    NULL, /* visit_corrupt */
    _bson_as_table_visit_double,
    _bson_as_table_visit_utf8,
    _bson_as_table_visit_document,
    _bson_as_table_visit_array,
    NULL, /* visit_binary */ 
    NULL, /* visit_undefined */
    _bson_as_table_visit_oid,
    _bson_as_table_visit_bool,
    _bson_as_table_visit_date_time,
    NULL, /* visit_null */
    NULL, /* visit_regex */
    NULL, /* visit_dbpointer */ 
    NULL, /* visit_code */ 
    NULL, /* visit_symbol */ 
    NULL, /* visit_codewscope */ 
    _bson_as_table_visit_int32,
    _bson_as_table_visit_timestamp,
    _bson_as_table_visit_int64,
    NULL, /* visit_maxkey */
    NULL, /* visit_minkey */ 
    NULL, /* visit_unsupported_type */
#ifdef BSON_EXPERIMENTAL_FEATURES
    NULL, /* visit_decimal128 */
#endif /* BSON_EXPERIMENTAL_FEATURES */
};

static bool
_bson_as_table_visit_document (const bson_iter_t *iter,
        const char        *key,
        const bson_t      *v_document,
        void              *data)
{
    bson_lua_state_t *state = data;
    lua_State *L = state->L;
    bson_lua_state_t child_state = { 0, L };
    bson_iter_t child;

    if (bson_iter_init (&child, v_document)) {
        _bson_as_table_push_key(key, data);
        lua_newtable(L);
        bson_iter_visit_all (&child, &bson_as_table_visitors, &child_state);
        lua_rawset(L, -3);
    }

    return false;
}

static bool
_bson_as_table_visit_array (const bson_iter_t *iter,
        const char        *key,
        const bson_t      *v_array,
        void              *data)
{
    bson_lua_state_t *state = data;
    lua_State *L = state->L;
    bson_lua_state_t child_state = { 1, L };
    bson_iter_t child;

    if (bson_iter_init (&child, v_array)) {
        _bson_as_table_push_key(key, data);
        lua_newtable(L);
        bson_iter_visit_all (&child, &bson_as_table_visitors, &child_state);
        lua_rawset(L, -3);
    }

    return false;
}

/*
 * bson转lua table
 * 转换结果直接压入到lua_State栈上
 * 若转换失败则使用luaL_error抛出异常信息
 */
void 
bson_as_table(lua_State *L, const bson_t *bson) {
    bson_iter_t iter;
    BSON_ASSERT (bson);

    lua_newtable(L);
    if (bson_empty0 (bson)) {
        return;
    }

    if (!bson_iter_init (&iter, bson)) {
        luaL_error(L, "(can't init bson iter)");
    }

    bson_lua_state_t state = { 0, L };
    if (bson_iter_visit_all (&iter, &bson_as_table_visitors, &state) ||
            iter.err_off) {
        luaL_error(L, "(can't convert bson to lua table)");
    }
}

static bool
_table_is_pure_array(lua_State *L, int idx) {
    int index = lua_absindex(L, idx);
    size_t len = lua_rawlen(L, index);
    bool isarray = false;
    if (len > 0) {
        lua_pushinteger(L, len);
        if (lua_next(L, index) == 0) {
            isarray = true;
        } else {
            lua_pop(L, 2);
        }
    }
    return isarray;
}

static void
_bson_append_val(bson_t *bson, lua_State *L, const char *key, int len) {
    int vt = lua_type(L, -1);
    switch(vt) {
        case LUA_TNUMBER:
            {
                lua_Integer lsh = lua_tointeger(L, -1);
                lua_Number rsh = lua_tonumber(L, -1);
                if (lsh == rsh) {
                    int64_t val = lua_tointeger(L, -1);
                    if (is_32bit(val)) {
                        bson_append_int32(bson, key, len, val);
                    } else {
                        bson_append_int64(bson, key, len, val);
                    }
                } else {
                    double val = lua_tonumber(L, -1);
                    bson_append_double(bson, key, len, val);
                }
                break;
            }
        case LUA_TSTRING: 
            {
                size_t sz;
                const char* val = lua_tolstring(L, -1, &sz);
                if (sz > 1 && val[0] == 0) {
                    bson_type_t t = val[1];
                    if (t == BSON_TYPE_OID) {
                        bson_oid_t oid;
                        bson_oid_init_from_string(&oid, val+2);
                        bson_append_oid(bson, key, len, &oid);
                    }
                } else {
                    // normal string
                    bson_append_utf8(bson, key, len, val, sz);
                }
                break;
            }
        case LUA_TBOOLEAN:
            {
                int val = lua_toboolean(L, -1);
                bson_append_bool(bson, key, len, val);
                break;
            }
        case LUA_TTABLE:
            {
                bool array = _table_is_pure_array(L, -1);
                bson_t *val = _table_as_bson(L, -1, array);
                if (array) {
                    bson_append_array(bson, key, len, val);
                } else {
                    bson_append_document(bson, key, len, val);
                }
                bson_destroy(val);
                break;
            }
        case LUA_TUSERDATA:
            {
                bson_wrap_t *ud = (bson_wrap_t*)luaL_checkudata(L, -1, "bson");
                bson_append_document(bson, key, len, ud->bson);
                break;
            }
    }
}

static bson_t *
_table_as_bson(lua_State *L, int idx, bool array) {
    bson_t *bson;
    bson = bson_new();
    int index = lua_absindex(L, idx);
    lua_pushnil(L);
    while (lua_next(L, index) != 0) {
        if (array) {
            int idx = lua_tointeger(L, -2);
            char keyidx[8] = {0};
            sprintf(keyidx, "%d", idx - 1);
            _bson_append_val(bson, L, keyidx, strlen(keyidx));
        } else {
            int kt = lua_type(L, -2);
            const char *key = NULL;
            size_t sz;
            switch(kt) {
                case LUA_TNUMBER:
                    // copy key, don't change key type
                    lua_pushvalue(L, -2);
                    lua_insert(L, -2);
                    key = lua_tolstring(L, -2, &sz);
                    _bson_append_val(bson, L, key, sz);
                    lua_pop(L, 1);
                    break;
                case LUA_TSTRING:
                    key = lua_tolstring(L, -2, &sz);
                    _bson_append_val(bson, L, key, sz);
                    break;
            }
        }

        lua_pop(L, 1);
    }
    return bson;
}

/*
 * lua table转bson
 * 返回bson指针
 * 若转换失败则使用luaL_error抛出异常信息
 */
bson_t *
table_as_bson(lua_State *L, int index) {
    luaL_checktype(L, index, LUA_TTABLE);
    bool array = _table_is_pure_array(L, index);
    if (array) {
        luaL_error(L, "array table is not supported.");
    }
    return _table_as_bson(L, index, false);
}

bson_t *
array_as_bson(lua_State *L, int num) {
    bson_t *bson;
    bson = bson_new();
    int i;
    for (i=0; i<num; i+=2) {
        size_t sz;
        const char * key = lua_tolstring(L, i+1, &sz);
        if (key == NULL) {
            luaL_error(L, "Argument %d need a string", i+1);
        }
        lua_pushvalue(L, i+2);
        _bson_append_val(bson, L, key, sz);
        lua_pop(L,1);
    }
    return bson;
}

/*
 * 从userdata中获取bson指针
 */
const bson_t *
userdata_as_bson(lua_State *L, int index) {
    if (!lua_isuserdata(L, index)) return NULL;
    bson_wrap_t *ud = (bson_wrap_t*)luaL_checkudata(L, index, "bson");
    return ud->bson;
}

static int
bson_gc(lua_State *L) {
    bson_wrap_t *ud = (bson_wrap_t*)luaL_checkudata(L, 1, "bson");
    bson_t *bson = ud->bson;
    bson_destroy(bson);
    return 0;
}

static void
bson_meta(lua_State *L) {
    if (luaL_newmetatable(L, "bson")) {
        lua_pushcfunction(L, bson_gc);
        lua_setfield(L, -2, "__gc");
    }
    lua_setmetatable(L, -2);
}

static int
op_encode(lua_State *L) {
    bson_t *bson = table_as_bson(L, -1);
    bson_wrap_t * ud = (bson_wrap_t*)lua_newuserdata(L, sizeof(bson_wrap_t));
    ud->bson = bson;
    bson_meta(L);
    return 1;
}

static int
op_encode_order(lua_State *L) {
    int n = lua_gettop(L);
    if (n%2 != 0) {
        luaL_error(L, "Invalid ordered dict");
    }

    bson_t *bson = array_as_bson(L, n);
    lua_settop(L,1);
    bson_wrap_t * ud = (bson_wrap_t*)lua_newuserdata(L, sizeof(bson_wrap_t));
    ud->bson = bson;
    bson_meta(L);
    return 1;
}

int
luaopen_mongo_bson(lua_State *L) {
    luaL_checkversion(L);
    luaL_Reg l[] ={
        { "encode", op_encode },
        { "encode_order", op_encode_order },
        { NULL,     NULL },
    };

    luaL_newlib(L,l);
    return 1;
}

