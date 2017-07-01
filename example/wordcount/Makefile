INC_DIR = ../../include
BUILD_PREFIX = ../../bin
TEMP_PREFIX = ../../tmp

CXX = g++
CFLAGS += -D _SUGGEST -D _ERROR -Wall -std=c++11 -I$(INC_DIR)# -D _DEBUG
LDFLAGS += -shared -fPIC
OBJS = sourceManager utils splitter
OBJS_PATHS = $(foreach OBJ, $(OBJS), $(TEMP_PREFIX)/$(OBJ).o)
EXECS = wordcount
EXECS_PATHS = $(foreach EXEC, $(EXECS), $(BUILD_PREFIX)/$(EXEC))

all: build $(EXECS)

$(EXECS): % : %.cpp $(OBJS_PATHS)
	$(CXX) $(CFLAGS) $(LDFLAGS) -o $(BUILD_PREFIX)/$@ $^

.PHONY: build
build:
	mkdir -p $(BUILD_PREFIX)
	mkdir -p $(TEMP_PREFIX)

.PHONY: clean
clean:
	rm -rf $(BUILD_PREFIX)/$(OBJ)
	@(rmdir $(BUILD_PREFIX) 2> /dev/null; exit 0)
	@(rmdir $(TEMP_PREFIX) 2> /dev/null; exit 0)
