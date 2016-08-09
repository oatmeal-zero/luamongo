IMPORT_CPPFLAGS += $(patsubst %,-I%/../3rd/mongo-c-driver/src/mongoc,$(IMPORT_TREES))
IMPORT_CPPFLAGS += $(patsubst %,-I%/../3rd/mongo-c-driver/src/libbson/src/bson,$(IMPORT_TREES))
IMPORT_LIBRARY_FLAGS += $(patsubst %,-L%/test/mongoc,$(IMPORT_TREES))

LIB_NAME := mongo
LIB_VERSION  := ""
LIB_OBJS := lua-mongo.o lua-bson.o

all:: mkshared

vers  := _ ""
major := ""

namespec := $(LIB_NAME) $(vers)

LIB_IMPORTS  := -lmongoc-1.0

##############################################################################
# Build Static library
##############################################################################

staticlib := lib$(LIB_NAME)$(major).a

mkstatic::
	@(dir=static; $(CreateDir))

mkstatic:: $(staticlib)
$(staticlib): $(patsubst %, static/%, $(LIB_OBJS))
	@$(StaticLinkLibrary)

##############################################################################
# Build Shared library
##############################################################################

shlib := $(LIB_NAME)$(major).so

imps  := $(LIB_IMPORTS)

mkshared::
	@(dir=shared; $(CreateDir))

mkshared:: $(shlib)
$(shlib): $(patsubst %, shared/%, $(LIB_OBJS))
	@(namespec="$(namespec)" extralibs="$(imps) $(extralibs)" nodeffile=1; \
	$(MakeCXXSharedLibrary))

install::
	@($(CP) $(shlib) $(TOP)/lib)

clean::
	$(RM) static/*.o shared/*.o *.d *.output
