INC_DIR = ../include

CXX = g++
CFLAGS += -D _DEBUG -D _SUGGEST -D _ERROR -Wall -std=c++11 -I$(INC_DIR)
LDFLAGS += -lpthread
TESTS = streamManager type
OBJS = $(foreach TEST, $(TESTS), test_$(TEST))

all: $(OBJS)

test_%: test_%.cpp $(INC_DIR)/%.hpp
	$(CXX) $(CFLAGS) $(LDFLAGS) $< -o $@

.PHONY: clean
clean:
	rm -rf $(OBJS)
