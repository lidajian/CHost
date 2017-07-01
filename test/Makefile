INC_DIR = ../include
SRC_DIR = ../src
TEMP_PREFIX = ../tmp
BUILD_PREFIX = ../tbin

CXX = g++
CFLAGS += -D _DEBUG -D _SUGGEST -D _ERROR -Wall -std=c++11 -I$(INC_DIR)
LDFLAGS += -lpthread
OBJS = sourceManager utils splitter threadPool
OBJS_PATHS = $(foreach OBJ, $(OBJS), $(TEMP_PREFIX)/$(OBJ).o)
TESTS = streamManager type threadPool
EXECS = $(foreach TEST, $(TESTS), test_$(TEST))

all: build $(OBJS) $(EXECS)

$(OBJS) : % : $(SRC_DIR)/%.cpp
	$(CXX) $(CFLAGS) -c $^ -o $(TEMP_PREFIX)/$@.o

test_%: test_%.cpp $(OBJS_PATHS)
	$(CXX) $(CFLAGS) $(LDFLAGS) $^ -o $(BUILD_PREFIX)/$@

.PHONY: build
build:
	mkdir -p $(TEMP_PREFIX)
	mkdir -p $(BUILD_PREFIX)

.PHONY: clean
clean:
	rm -rf $(EXECS) $(OBJS_PATHS)
	@(rmdir $(TEMP_PREFIX) 2> /dev/null; exit 0)
	@(rmdir $(BUILD_PREFIX) 2> /dev/null; exit 0)
