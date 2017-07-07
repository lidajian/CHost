EXAMPLES_DIR = ./example
TEST_DIR = ./test
INC_DIR = ./include
BUILD_PREFIX = ./bin
TEMP_PREFIX = ./tmp
SRC_DIR = ./src

CXX = g++
CFLAGS += -D _SUGGEST -D _ERROR -Wall -fPIC -std=c++11 -I$(INC_DIR)
# CFLAGS += -D _DEBUG
# CFLAGS += -D MULTIPLE_MAPPER
LDFLAGS += -lpthread -ldl
OBJS = sourceManager utils splitter threadPool
OBJS_PATHS = $(foreach OBJ, $(OBJS), $(TEMP_PREFIX)/$(OBJ).o)
EXECS = chserver chrun

all: build_dir $(OBJS) $(EXECS)

$(OBJS) : % : $(SRC_DIR)/%.cpp
	$(CXX) $(CFLAGS) -c $^ -o $(TEMP_PREFIX)/$@.o

$(EXECS) : % : $(SRC_DIR)/%.cpp $(OBJS_PATHS) 
	$(CXX) $(CFLAGS) $(LDFLAGS) -o $(BUILD_PREFIX)/$@ $^

.PHONY: build_dir
build_dir:
	mkdir -p $(TEMP_PREFIX)
	mkdir -p $(BUILD_PREFIX)

.PHONY: example
example:
	cd $(EXAMPLES_DIR) && make

.PHONY: test
test:
	cd $(TEST_DIR) && make

.PHONY: clean_example
clean_example:
	cd $(EXAMPLES_DIR) && make clean

.PHONY: clean_test
clean_test:
	cd $(TEST_DIR) && make clean

.PHONY: clean_temp
clean_temp:
	rm -rf $(TEMP_PREFIX)

clean: clean_temp clean_example clean_test
	rm -rf $(BUILD_PREFIX)
