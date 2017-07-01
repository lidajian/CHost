EXAMPLES_DIR = ./example
TEST_DIR = ./test
INC_DIR = ./include
BUILD_PREFIX = ./bin
TEMP_PREFIX = ./tmp
SRC_DIR = ./src

CXX = g++
CFLAGS += -D _SUGGEST -D _ERROR -Wall -std=c++11 -I$(INC_DIR)
LDFLAGS += -lpthread -ldl
OBJS = sourceManager utils splitter threadPool
OBJS_PATHS = $(foreach OBJ, $(OBJS), $(TEMP_PREFIX)/$(OBJ).o)
EXECS = chserver chrun
EXECS_PATHS = $(foreach EXEC, $(EXECS), $(BUILD_PREFIX)/$(EXEC))

all: build $(OBJS) $(EXECS) example clear_temp

$(OBJS) : % : $(SRC_DIR)/%.cpp
	$(CXX) $(CFLAGS) -c $^ -o $(TEMP_PREFIX)/$@.o

$(EXECS) : % : $(SRC_DIR)/%.cpp $(OBJS_PATHS) 
	$(CXX) $(CFLAGS) $(LDFLAGS) -o $(BUILD_PREFIX)/$@ $^

.PHONY: build
build:
	mkdir -p $(TEMP_PREFIX)
	mkdir -p $(BUILD_PREFIX)

.PHONY: example
example:
	cd $(EXAMPLES_DIR) && make

.PHONY: test
test:
	cd $(TEST_DIR) && make

.PHONY: clear_temp
clear_temp:
	rm -rf $(TEMP_PREFIX)

.PHONY: clean
clean:
	rm -rf $(OBJS_PATHS) $(EXECS_PATHS)
	rm -rf $(TEMP_PREFIX)
	@(rmdir $(BUILD_PREFIX) 2> /dev/null; exit 0)
	@(rmdir $(TEMP_PREFIX) 2> /dev/null; exit 0)
	cd $(EXAMPLES_DIR) && make clean
