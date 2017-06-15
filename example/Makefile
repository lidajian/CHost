OBJS = wordcount

all: $(OBJS)
	for f in $^; do cd $$f && make; done

.PHONY: clean
clean:
	for f in $(OBJS); do cd $$f && make clean; done
