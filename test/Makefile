INC_DIR = ../include
SRC_DIR = ../src
TEMP_PREFIX = ../tmp
BUILD_PREFIX = ../tbin

CXX = g++
CFLAGS += -D _DEBUG -D _SUGGEST -D _ERROR -Wall -fPIC -std=c++1z -I$(INC_DIR)
LDFLAGS += -lpthread
OBJS = sourceManager utils splitter threadPool
OBJS_PATHS = $(foreach OBJ, $(OBJS), $(TEMP_PREFIX)/$(OBJ).o)
TESTS = streamManager type threadPool
EXECS = $(foreach TEST, $(TESTS), test_$(TEST))

all: build $(OBJS) $(EXECS) clean_temp

$(OBJS) : % : $(SRC_DIR)/%.cpp
	$(CXX) $(CFLAGS) -c $^ -o $(TEMP_PREFIX)/$@.o

test_%: test_%.cpp $(OBJS_PATHS)
	$(CXX) $(CFLAGS) $(LDFLAGS) $^ -o $(BUILD_PREFIX)/$@

.PHONY: build
build:
	mkdir -p $(TEMP_PREFIX)
	mkdir -p $(BUILD_PREFIX)

.PHONY: clean_temp
clean_temp:
	rm -rf $(TEMP_PREFIX)

clean: clean_temp
	rm -rf $(BUILD_PREFIX)
