INC_DIR = ../../include
BUILD_PREFIX = ../../bin
TEMP_PREFIX = ../../tmp

CXX = g++
CFLAGS += -D _SUGGEST -D _ERROR -Wall -std=c++11 -fPIC -I$(INC_DIR)# -D _DEBUG
LDFLAGS += -shared
OBJS = sourceManager utils splitter threadPool
OBJS_PATHS = $(foreach OBJ, $(OBJS), $(TEMP_PREFIX)/$(OBJ).o)
EXECS = wordcount
EXECS_PATHS = $(foreach EXEC, $(EXECS), $(BUILD_PREFIX)/$(EXEC))

all: build $(EXECS)

$(EXECS): % : %.cpp $(OBJS_PATHS)
	$(CXX) $(CFLAGS) $(LDFLAGS) -o $(BUILD_PREFIX)/$@ $^

.PHONY: build
build:
	mkdir -p $(BUILD_PREFIX)

.PHONY: clean
clean:
	rm -rf $(EXECS_PATHS)
	@(rmdir $(BUILD_PREFIX) 2> /dev/null; exit 0)
