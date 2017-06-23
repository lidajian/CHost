EXAMPLES_DIR = ./example
TEST_DIR = ./test
INC_DIR = ./include
BUILD_PREFIX = ./bin
SRC_DIR = ./src

CXX = g++
CFLAGS += -D _SUGGEST -D _ERROR -Wall -std=c++11 -I$(INC_DIR)
LDFLAGS += -lpthread -ldl
OBJS = chserver chrun

all: $(OBJS)

$(OBJS) : % : $(SRC_DIR)/%.cpp $(INC_DIR)/*.hpp
	mkdir -p $(BUILD_PREFIX)
	$(CXX) $(CFLAGS) $(LDFLAGS) -o $(BUILD_PREFIX)/$@ $<

.PHONY: example
example:
	cd $(EXAMPLES_DIR) && make

.PHONY: test
test:
	cd $(TEST_DIR) && make

.PHONY: clean
clean:
	rm -rf $(foreach OBJ, $(OBJS), $(BUILD_PREFIX)/$(OBJ))
	@(rmdir $(BUILD_PREFIX) 2> /dev/null; exit 0)
